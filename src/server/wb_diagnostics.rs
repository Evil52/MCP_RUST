//! Local completeness checks; absent or malformed source fields remain unknown.
use serde_json::{Value, json};

pub(super) fn diagnostics(data: &Value) -> Result<Value, String> {
    let cards = data
        .get("cards")
        .and_then(Value::as_array)
        .filter(|cards| cards.iter().all(Value::is_object))
        .ok_or("WB returned an invalid cards response; completeness is unknown")?;
    let rows: Vec<Value> = cards.iter().map(card_diagnostics).collect();
    Ok(json!({
        "coverage": "single_page",
        "checks_scope": "local_content_completeness_not_moderation",
        "unknown_value": null,
        "checked_count": rows.len(),
        "cards": rows,
        "cursor": data.get("cursor"),
    }))
}

fn card_diagnostics(card: &Value) -> Value {
    let checks = json!({
        "title": nonempty_text(card.get("title")),
        "description": nonempty_text(card.get("description")),
        "photos": photos(card.get("photos")),
        "characteristics": nonempty_array(card.get("characteristics")),
        "dimensions": dimensions(card.get("dimensions")),
        "weight": card.pointer("/dimensions/weightBrutto").and_then(Value::as_f64).map(|n| n > 0.0),
        "barcodes": barcodes(card.get("sizes")),
    });
    let findings: Vec<&str> = checks
        .as_object()
        .expect("object literal")
        .iter()
        .filter_map(|(field, value)| (value == false).then_some(field.as_str()))
        .collect();
    json!({
        "nmID": card.get("nmID"), "vendorCode": card.get("vendorCode"),
        "subjectID": card.get("subjectID"), "checks": checks, "findings": findings,
    })
}

fn nonempty_text(value: Option<&Value>) -> Option<bool> {
    value.and_then(Value::as_str).map(|s| !s.trim().is_empty())
}

fn nonempty_array(value: Option<&Value>) -> Option<bool> {
    value.and_then(Value::as_array).map(|v| !v.is_empty())
}

fn photos(value: Option<&Value>) -> Option<bool> {
    let photos = value?.as_array()?;
    if photos.is_empty() {
        return Some(false);
    }
    let checks: Option<Vec<bool>> = photos
        .iter()
        .map(|photo| nonempty_text(photo.get("big")))
        .collect();
    checks.map(|checks| checks.iter().all(|present| *present))
}

fn dimensions(value: Option<&Value>) -> Option<bool> {
    let value = value?.as_object()?;
    let lengths: Option<Vec<f64>> = ["length", "width", "height"]
        .iter()
        .map(|key| value.get(*key)?.as_f64())
        .collect();
    lengths.map(|lengths| lengths.iter().all(|n| *n > 0.0))
}

fn barcodes(value: Option<&Value>) -> Option<bool> {
    let sizes = value?.as_array()?;
    if sizes.is_empty() {
        return Some(false);
    }
    let checks: Option<Vec<bool>> = sizes
        .iter()
        .map(|size| {
            let skus = size.get("skus")?.as_array()?;
            let codes: Option<Vec<&str>> = skus.iter().map(Value::as_str).collect();
            codes.map(|codes| !codes.is_empty() && codes.iter().all(|s| !s.trim().is_empty()))
        })
        .collect();
    checks.map(|checks| checks.iter().all(|present| *present))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_data_is_unknown_and_empty_fields_are_findings() {
        let result = diagnostics(&json!({"cards":[{"nmID":1,"title":"", "photos":[],
            "dimensions":{"length":1,"width":1,"height":1,"weightBrutto":0},
            "sizes":[{"skus":["123"]}]}], "cursor":{"total":1,"nmID":1}}))
        .unwrap();
        assert_eq!(result["cards"][0]["checks"]["description"], Value::Null);
        assert_eq!(result["cards"][0]["checks"]["title"], false);
        assert_eq!(result["cards"][0]["checks"]["dimensions"], true);
        assert_eq!(result["cards"][0]["checks"]["weight"], false);
        assert_eq!(result["cards"][0]["checks"]["barcodes"], true);
        assert!(
            !result["cards"][0]["findings"]
                .as_array()
                .unwrap()
                .contains(&json!("description"))
        );
        assert_eq!(result["cursor"]["total"], 1);
        assert_eq!(result["coverage"], "single_page");
    }

    #[test]
    fn malformed_data_never_becomes_a_clean_empty_report() {
        for data in [
            json!({}),
            json!({"cards":null}),
            json!({"cards":[null]}),
            json!({"error":true}),
        ] {
            assert!(diagnostics(&data).is_err());
        }
        assert_eq!(
            diagnostics(&json!({"cards":[]})).unwrap()["checked_count"],
            0
        );
        assert_eq!(barcodes(Some(&json!([{"skus":[null]}]))), None);
        assert_eq!(photos(Some(&json!([{}]))), None);
        assert_eq!(
            photos(Some(&json!([{"big":"https://example.invalid/photo"}]))),
            Some(true)
        );
        assert_eq!(dimensions(Some(&json!({"length":1}))), None);
    }
}
