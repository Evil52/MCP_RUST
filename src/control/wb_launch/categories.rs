//! Content reads use the existing validated reader, never the writer token.
//! Missing category access/evidence is a blocker, not permission to guess.
use super::{ACCOUNT, NMS, Operator};
use crate::wb::WbClient;
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::collections::BTreeMap;

impl Operator {
    pub(super) async fn verify_category(&self) -> Result<u64> {
        verify_categories(&self.reader).await
    }
}

async fn verify_categories(reader: &WbClient) -> Result<u64> {
    let mut subjects = BTreeMap::new();
    for nm in NMS {
        let response = reader.product_cards(
                ACCOUNT,
                Some("ru".to_owned()),
                json!({"settings": {
                    "cursor": {"limit": 100},
                    "filter": {"textSearch": nm.to_string(), "withPhoto": -1}
                }}),
            ).await.context("category preflight requires current product cards through the existing reader; do not substitute the writer or widen credentials")?;
        subjects.insert(nm, subject_for_card(&response, nm)?);
    }
    require_single_subject(&subjects)
}

fn subject_for_card(response: &Value, nm: u64) -> Result<u64> {
    let cards = response
        .get("cards")
        .and_then(Value::as_array)
        .context("category preflight: missing cards")?;
    ensure!(
        cards.len() <= 100,
        "category response exceeds requested bound"
    );
    let matches = cards
        .iter()
        .filter(|card| card.get("nmID").and_then(Value::as_u64) == Some(nm))
        .collect::<Vec<_>>();
    ensure!(
        matches.len() == 1,
        "category preflight: SKU {nm} missing or duplicated"
    );
    matches[0]
        .get("subjectID")
        .and_then(Value::as_u64)
        .filter(|id| *id > 0)
        .context("category preflight: invalid subjectID")
}

fn require_single_subject(subjects: &BTreeMap<u64, u64>) -> Result<u64> {
    ensure!(
        subjects.keys().copied().eq(NMS),
        "category evidence does not cover the exact five SKUs"
    );
    let subject = subjects
        .values()
        .next()
        .copied()
        .context("category evidence is empty")?;
    ensure!(
        subject > 0 && subjects.values().all(|value| *value == subject),
        "CPC requires one category; current SKU/subject IDs: {subjects:?}. Choose a compatible set before authorizing a new campaign; no write attempted"
    );
    Ok(subject)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{test_support::mock_http, wb::WbCredentials};
    use std::time::Duration;

    fn reader(url: &str) -> WbClient {
        WbClient::new_for_test(
            Duration::from_secs(2),
            BTreeMap::from([(
                ACCOUNT.to_owned(),
                WbCredentials {
                    token: "reader-test-only".to_owned(),
                },
            )]),
            url,
            url,
        )
    }

    #[tokio::test]
    async fn category_reads_are_exact_and_never_use_writer_routes() {
        let (url, requests) = mock_http(
            NMS.into_iter()
                .map(|nm| {
                    (
                        200,
                        json!({"cards":[{"nmID":nm,"subjectID":4263}]}).to_string(),
                    )
                })
                .collect(),
        );
        assert_eq!(verify_categories(&reader(&url)).await.unwrap(), 4263);
        for nm in NMS {
            let request = requests.recv_timeout(Duration::from_secs(2)).unwrap();
            assert!(request.starts_with("POST /content/v2/get/cards/list?locale=ru "));
            let body: Value =
                serde_json::from_str(request.split("\r\n\r\n").nth(1).unwrap()).unwrap();
            assert_eq!(body["settings"]["filter"]["textSearch"], nm.to_string());
            assert_eq!(body["settings"]["cursor"]["limit"], 100);
        }
        assert!(requests.try_recv().is_err());
    }

    #[tokio::test]
    async fn denied_category_read_stops_without_retry_or_fallback() {
        let (url, requests) = mock_http(vec![(401, "{}".to_owned())]);
        assert!(verify_categories(&reader(&url)).await.is_err());
        assert!(
            requests
                .recv_timeout(Duration::from_secs(2))
                .unwrap()
                .starts_with("POST /content/v2/get/cards/list?locale=ru ")
        );
        assert!(requests.try_recv().is_err());
    }

    #[test]
    fn original_selection_is_rejected_before_creation() {
        let subjects = BTreeMap::from([
            (146_312_604, 6341),
            (207_418_966, 4263),
            (455_101_276, 4263),
            (461_126_890, 7354),
            (529_996_417, 4263),
        ]);
        let error = require_single_subject(&subjects).unwrap_err().to_string();
        assert!(error.contains("CPC requires one category"));
        assert!(error.contains("no write attempted"));
    }

    #[test]
    fn only_complete_single_subject_evidence_is_accepted() {
        let mut subjects = NMS
            .into_iter()
            .map(|nm| (nm, 4263))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(require_single_subject(&subjects).unwrap(), 4263);
        subjects.remove(&NMS[0]);
        assert!(require_single_subject(&subjects).is_err());
        subjects.insert(NMS[0], 0);
        assert!(require_single_subject(&subjects).is_err());
        subjects.insert(123, 4263);
        assert!(require_single_subject(&subjects).is_err());
    }

    #[test]
    fn card_identity_and_subject_must_be_unambiguous() {
        let card = json!({"nmID": NMS[0], "subjectID":4263});
        assert_eq!(
            subject_for_card(&json!({"cards":[card]}), NMS[0]).unwrap(),
            4263
        );
        for response in [
            Value::Null,
            json!({"cards":[]}),
            json!({"cards":[card.clone(),card]}),
            json!({"cards":[{"nmID":NMS[1],"subjectID":4263}]}),
            json!({"cards":[{"nmID":NMS[0],"subjectID":0}]}),
            json!({"cards":[{"nmID":NMS[0],"subjectID":"4263"}]}),
        ] {
            assert!(subject_for_card(&response, NMS[0]).is_err());
        }
    }
}
