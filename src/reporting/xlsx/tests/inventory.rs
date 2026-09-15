//! Inventory export preserves unknown quantities independently from prices.
use super::*;
use std::io::Read;

#[test]
fn missing_optional_metrics_are_written_as_nd() {
    let mut no_rates = kpis();
    no_rates.ctr = None;
    no_rates.cpc_minor = None;
    no_rates.ad_conversion = None;
    no_rates.cpo_minor = None;
    no_rates.drr = None;
    let inventory = [InventoryDetail {
        account_id: "ozon_store",
        sku: "sku",
        sellable_stock: 0,
        stock_observed: false,
        price_minor: None,
        observed_at: generated_at(),
    }];
    let sales = [SalesDetail {
        account_id: "ozon_store",
        sku: "sku",
        ordered_units: 1,
        operational_gmv_minor: 100,
        cancelled_units: None,
        returned_units: None,
    }];
    let bytes = render_xlsx(XlsxReport {
        summary: summary(&no_rates, &[]),
        sales: &sales,
        advertising: &[],
        inventory: &inventory,
        source_quality: &[],
    })
    .unwrap();
    assert!(bytes.starts_with(b"PK"));
}

#[test]
fn inventory_export_writes_nd_for_unobserved_stock_and_numeric_zero_for_observed_zero() {
    let inventory = [
        InventoryDetail {
            account_id: "ozon_store",
            sku: "unknown",
            sellable_stock: 0,
            stock_observed: false,
            price_minor: Some(1_000),
            observed_at: generated_at(),
        },
        InventoryDetail {
            account_id: "ozon_store",
            sku: "observed-zero",
            sellable_stock: 0,
            stock_observed: true,
            price_minor: None,
            observed_at: generated_at(),
        },
    ];
    let kpis = kpis();
    let bytes = render_xlsx(XlsxReport {
        summary: summary(&kpis, &[]),
        sales: &[],
        advertising: &[],
        inventory: &inventory,
        source_quality: &[],
    })
    .unwrap();
    let strings = zip_xml(&bytes, "xl/sharedStrings.xml");
    let nd_index = strings
        .split("<si>")
        .skip(1)
        .position(|entry| entry.contains("<t>N/D</t>"))
        .unwrap();
    let sheet = zip_xml(&bytes, "xl/worksheets/sheet4.xml");
    assert!(
        sheet.contains(&format!("<c r=\"C2\" t=\"s\"><v>{nd_index}</v></c>")),
        "unknown stock must reference N/D: {sheet}"
    );
    assert!(
        sheet.contains("<c r=\"C3\"><v>0</v></c>"),
        "observed zero must remain numeric: {sheet}"
    );
}

// Read only generated XLSX ZIP entries, using the existing flate2 test dependency.
fn zip_xml(archive: &[u8], requested: &str) -> String {
    fn short(bytes: &[u8], offset: usize) -> usize {
        usize::from(u16::from_le_bytes(
            bytes[offset..offset + 2].try_into().unwrap(),
        ))
    }
    fn long(bytes: &[u8], offset: usize) -> usize {
        usize::try_from(u32::from_le_bytes(
            bytes[offset..offset + 4].try_into().unwrap(),
        ))
        .unwrap()
    }
    for (offset, signature) in archive.windows(4).enumerate() {
        if signature != b"PK\x01\x02" {
            continue;
        }
        let header = &archive[offset..];
        let name_length = short(header, 28);
        if &header[46..46 + name_length] != requested.as_bytes() {
            continue;
        }
        let local = &archive[long(header, 42)..];
        let body_start = 30 + short(local, 26) + short(local, 28);
        let body = &local[body_start..body_start + long(header, 20)];
        return match short(header, 10) {
            0 => String::from_utf8(body.to_vec()).unwrap(),
            8 => {
                let mut output = String::new();
                flate2::read::DeflateDecoder::new(body)
                    .read_to_string(&mut output)
                    .unwrap();
                output
            }
            method => panic!("unexpected XLSX compression: {method}"),
        };
    }
    panic!("missing XLSX entry {requested}");
}
