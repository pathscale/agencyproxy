use agency_proxy_protocol::MAX_FRAME_BYTES;
use serde::{Serialize, de::DeserializeOwned};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};

use crate::Error;

pub async fn read_frame<T>(reader: &mut (impl AsyncBufRead + Unpin)) -> Result<Option<T>, Error>
where
    T: DeserializeOwned,
{
    let mut bytes = Vec::new();
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return if bytes.is_empty() {
                Ok(None)
            } else {
                Err(Error::TruncatedFrame)
            };
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |index| index + 1);
        let payload = newline.map_or(available, |index| &available[..index]);
        if bytes.len() + payload.len() > MAX_FRAME_BYTES {
            return Err(Error::FrameTooLarge);
        }
        bytes.extend_from_slice(payload);
        reader.consume(consumed);
        if newline.is_some() {
            return serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(Error::from);
        }
    }
}

pub async fn write_frame<T>(writer: &mut (impl AsyncWrite + Unpin), frame: &T) -> Result<(), Error>
where
    T: Serialize,
{
    let bytes = serde_json::to_vec(frame)?;
    if bytes.len() > MAX_FRAME_BYTES {
        return Err(Error::FrameTooLarge);
    }
    writer.write_all(&bytes).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use agency_proxy_protocol::{ClientFrame, ClientMessage};
    use tokio::io::BufReader;

    #[tokio::test]
    async fn rejects_a_frame_before_unbounded_buffer_growth() {
        let source = vec![b'x'; MAX_FRAME_BYTES + 1];
        let mut reader = BufReader::new(source.as_slice());
        let error = read_frame::<ClientFrame>(&mut reader)
            .await
            .expect_err("oversized frame should fail");
        assert!(matches!(error, Error::FrameTooLarge));
    }

    #[tokio::test]
    async fn reads_exactly_one_newline_delimited_frame() {
        let frame = ClientFrame {
            request_id: 1,
            message: ClientMessage::ListRuns,
        };
        let mut bytes = serde_json::to_vec(&frame).expect("frame should encode");
        bytes.push(b'\n');
        let mut reader = BufReader::new(bytes.as_slice());
        assert_eq!(
            read_frame(&mut reader).await.expect("read should work"),
            Some(frame)
        );
    }
}
