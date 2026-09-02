use ipir_sp::server::CrsBlock;

const EVAL_REQUEST_MAGIC: &[u8; 4] = b"MPQ1";
const EVAL_RESPONSE_MAGIC: &[u8; 4] = b"MPR1";
const HINT_MAGIC: &[u8; 4] = b"MPH1";
const MAX_SHARDS_PER_WORKER: usize = 4_096;

#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("malformed wire message: {0}")]
    Malformed(String),
}

#[derive(Debug, Clone)]
pub struct ShardQuery {
    pub shard_id: u64,
    pub coefficients: Vec<u64>,
}

#[derive(Debug, Clone)]
pub struct EvaluateRequest {
    pub generation: u64,
    pub shards: Vec<ShardQuery>,
}

pub fn encode_evaluate_request(request: &EvaluateRequest) -> Vec<u8> {
    let coefficient_count: usize = request
        .shards
        .iter()
        .map(|shard| shard.coefficients.len())
        .sum();
    let mut output = Vec::with_capacity(16 + request.shards.len() * 8 + coefficient_count * 8);
    output.extend_from_slice(EVAL_REQUEST_MAGIC);
    output.extend_from_slice(&request.generation.to_le_bytes());
    output.extend_from_slice(&(request.shards.len() as u32).to_le_bytes());
    for shard in &request.shards {
        output.extend_from_slice(&shard.shard_id.to_le_bytes());
        output.extend_from_slice(&(shard.coefficients.len() as u32).to_le_bytes());
        for coefficient in &shard.coefficients {
            output.extend_from_slice(&coefficient.to_le_bytes());
        }
    }
    output
}

pub fn decode_evaluate_request(bytes: &[u8]) -> Result<EvaluateRequest, WireError> {
    let mut input = Input::new(bytes);
    input.expect_magic(EVAL_REQUEST_MAGIC)?;
    let generation = input.u64()?;
    let shard_count = input.u32()? as usize;
    if shard_count == 0 || shard_count > MAX_SHARDS_PER_WORKER {
        return Err(WireError::Malformed("invalid shard count".to_string()));
    }
    let mut shards = Vec::with_capacity(shard_count);
    for _ in 0..shard_count {
        let shard_id = input.u64()?;
        let coefficient_count = input.u32()? as usize;
        let byte_count = coefficient_count
            .checked_mul(8)
            .ok_or_else(|| WireError::Malformed("coefficient length overflow".to_string()))?;
        let raw = input.take(byte_count)?;
        let coefficients = raw
            .chunks_exact(8)
            .map(|chunk| u64::from_le_bytes(chunk.try_into().expect("eight-byte chunk")))
            .collect();
        shards.push(ShardQuery {
            shard_id,
            coefficients,
        });
    }
    input.finish()?;
    Ok(EvaluateRequest { generation, shards })
}

pub fn encode_evaluate_response(generation: u64, coefficients: &[u64]) -> Vec<u8> {
    let mut output = Vec::with_capacity(16 + coefficients.len() * 8);
    output.extend_from_slice(EVAL_RESPONSE_MAGIC);
    output.extend_from_slice(&generation.to_le_bytes());
    output.extend_from_slice(&(coefficients.len() as u32).to_le_bytes());
    for coefficient in coefficients {
        output.extend_from_slice(&coefficient.to_le_bytes());
    }
    output
}

pub fn decode_evaluate_response(bytes: &[u8]) -> Result<(u64, Vec<u64>), WireError> {
    let mut input = Input::new(bytes);
    input.expect_magic(EVAL_RESPONSE_MAGIC)?;
    let generation = input.u64()?;
    let count = input.u32()? as usize;
    let raw = input.take(
        count
            .checked_mul(8)
            .ok_or_else(|| WireError::Malformed("response length overflow".to_string()))?,
    )?;
    input.finish()?;
    let values = raw
        .chunks_exact(8)
        .map(|chunk| u64::from_le_bytes(chunk.try_into().expect("eight-byte chunk")))
        .collect();
    Ok((generation, values))
}

pub fn encode_crs_blocks(blocks: &[CrsBlock]) -> Vec<u8> {
    let row_count: usize = blocks.iter().map(|block| block.rows.len()).sum();
    let coefficient_count: usize = blocks
        .iter()
        .flat_map(|block| &block.rows)
        .map(Vec::len)
        .sum();
    let mut output = Vec::with_capacity(8 + row_count * 4 + coefficient_count * 8);
    output.extend_from_slice(HINT_MAGIC);
    output.extend_from_slice(&(blocks.len() as u32).to_le_bytes());
    for block in blocks {
        output.extend_from_slice(&(block.rows.len() as u32).to_le_bytes());
        for row in &block.rows {
            output.extend_from_slice(&(row.len() as u32).to_le_bytes());
            for coefficient in row {
                output.extend_from_slice(&coefficient.to_le_bytes());
            }
        }
    }
    output
}

pub fn decode_crs_blocks(
    bytes: &[u8],
    expected_blocks: usize,
    degree: usize,
) -> Result<Vec<CrsBlock>, WireError> {
    let mut input = Input::new(bytes);
    input.expect_magic(HINT_MAGIC)?;
    let blocks = input.u32()? as usize;
    if blocks != expected_blocks {
        return Err(WireError::Malformed(
            "unexpected CRS block count".to_string(),
        ));
    }
    let mut output = Vec::with_capacity(blocks);
    for _ in 0..blocks {
        if input.u32()? as usize != degree {
            return Err(WireError::Malformed("unexpected CRS row count".to_string()));
        }
        let mut rows = Vec::with_capacity(degree);
        for _ in 0..degree {
            if input.u32()? as usize != degree {
                return Err(WireError::Malformed(
                    "unexpected CRS coefficient count".to_string(),
                ));
            }
            let raw = input.take(degree.checked_mul(8).ok_or_else(|| {
                WireError::Malformed("CRS coefficient length overflow".to_string())
            })?)?;
            rows.push(
                raw.chunks_exact(8)
                    .map(|chunk| u64::from_le_bytes(chunk.try_into().expect("eight-byte chunk")))
                    .collect(),
            );
        }
        output.push(CrsBlock { rows });
    }
    input.finish()?;
    Ok(output)
}

struct Input<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Input<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], WireError> {
        let end = self
            .position
            .checked_add(count)
            .ok_or_else(|| WireError::Malformed("offset overflow".to_string()))?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| WireError::Malformed("truncated message".to_string()))?;
        self.position = end;
        Ok(value)
    }

    fn expect_magic(&mut self, expected: &[u8; 4]) -> Result<(), WireError> {
        if self.take(4)? != expected {
            return Err(WireError::Malformed("wrong message magic".to_string()));
        }
        Ok(())
    }

    fn u32(&mut self) -> Result<u32, WireError> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("four-byte slice"),
        ))
    }

    fn u64(&mut self) -> Result<u64, WireError> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("eight-byte slice"),
        ))
    }

    fn finish(self) -> Result<(), WireError> {
        if self.position != self.bytes.len() {
            return Err(WireError::Malformed("trailing message bytes".to_string()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evaluate_wire_round_trips_and_rejects_trailing_bytes() {
        let request = EvaluateRequest {
            generation: 9,
            shards: vec![ShardQuery {
                shard_id: 3,
                coefficients: vec![1, 2, 3],
            }],
        };
        let bytes = encode_evaluate_request(&request);
        let decoded = decode_evaluate_request(&bytes).expect("decode");
        assert_eq!(decoded.generation, 9);
        assert_eq!(decoded.shards[0].coefficients, vec![1, 2, 3]);
        let mut malformed = bytes;
        malformed.push(0);
        assert!(decode_evaluate_request(&malformed).is_err());
    }
}
