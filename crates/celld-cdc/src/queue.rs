//! SQS, and the S3 event notifications that arrive on it.

use anyhow::{Context, Result};
use aws_sdk_sqs::types::Message;
use aws_sdk_sqs::Client;
use celld_cdc_core::key::decode_event_key;
use serde::Deserialize;

pub struct Queue {
    client: Client,
    url: String,
}

#[derive(Deserialize)]
struct Notification {
    #[serde(rename = "Records", default)]
    records: Vec<Record>,
}

#[derive(Deserialize)]
struct Record {
    s3: S3Entity,
}

#[derive(Deserialize)]
struct S3Entity {
    object: S3Object,
}

#[derive(Deserialize)]
struct S3Object {
    key: String,
}

/// The decoded object keys an S3 notification names. A test event, or a
/// body that is not a notification, names none.
pub fn object_keys(body: &str) -> Vec<String> {
    serde_json::from_str::<Notification>(body)
        .map(|n| n.records.into_iter().map(|r| decode_event_key(&r.s3.object.key)).collect())
        .unwrap_or_default()
}

impl Queue {
    /// Connect with the standard AWS configuration; `AWS_ENDPOINT_URL`
    /// selects LocalStack.
    pub async fn connect(url: &str) -> Result<Self> {
        let config = aws_config::load_from_env().await;
        Ok(Self {
            client: Client::new(&config),
            url: url.to_string(),
        })
    }

    /// Long-poll for up to `max` messages.
    pub async fn receive(&self, max: i32, wait_secs: i32) -> Result<Vec<Message>> {
        let out = self
            .client
            .receive_message()
            .queue_url(&self.url)
            .max_number_of_messages(max.clamp(1, 10))
            .wait_time_seconds(wait_secs)
            .send()
            .await
            .context("receive from sqs")?;
        Ok(out.messages.unwrap_or_default())
    }

    pub async fn delete(&self, m: &Message) -> Result<()> {
        if let Some(handle) = m.receipt_handle() {
            self.client
                .delete_message()
                .queue_url(&self.url)
                .receipt_handle(handle)
                .send()
                .await
                .context("delete sqs message")?;
        }
        Ok(())
    }

    /// Make a message visible again after `secs`, to retry it.
    pub async fn retry_in(&self, m: &Message, secs: i32) -> Result<()> {
        if let Some(handle) = m.receipt_handle() {
            self.client
                .change_message_visibility()
                .queue_url(&self.url)
                .receipt_handle(handle)
                .visibility_timeout(secs)
                .send()
                .await
                .context("change sqs visibility")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_keys_from_a_notification() {
        let body = r#"{"Records":[{"eventName":"ObjectCreated:Put","s3":{"bucket":{"name":"celld"},
            "object":{"key":"cells/Counter%3Aab/ltx/e1/0000/0000000000000001-0000000000000001.ltx","size":1}}}]}"#;
        assert_eq!(
            object_keys(body),
            vec!["cells/Counter:ab/ltx/e1/0000/0000000000000001-0000000000000001.ltx"]
        );
    }

    #[test]
    fn a_test_event_names_nothing() {
        let body = r#"{"Service":"Amazon S3","Event":"s3:TestEvent","Bucket":"celld"}"#;
        assert!(object_keys(body).is_empty());
    }
}
