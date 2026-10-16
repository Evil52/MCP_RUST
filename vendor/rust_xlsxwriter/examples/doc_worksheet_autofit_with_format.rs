// SPDX-License-Identifier: MIT OR Apache-2.0
//
// Copyright 2022-2026, John McNamara, jmcnamara@cpan.org

//! The following example demonstrates autofitting data with different number
//! formats. This required the `enhanced_autofit` feature to be enabled.

use rust_xlsxwriter::{Format, Workbook, XlsxError};

fn main() -> Result<(), XlsxError> {
    let mut workbook = Workbook::new();

    // Add a worksheet to the workbook.
    let worksheet = workbook.add_worksheet();

    // Create some formats to use with the data below.
    let format1 = Format::new().set_num_format("0");
    let format2 = Format::new().set_num_format("000");
    let format3 = Format::new().set_num_format("00000");
    let format4 = Format::new().set_num_format("0000000");
    let format5 = Format::new().set_num_format("000000000");

    // Write a number with different Excel formats.
    worksheet.write_with_format(0, 0, 1, &format1)?;
    worksheet.write_with_format(0, 1, 1, &format2)?;
    worksheet.write_with_format(0, 2, 1, &format3)?;
    worksheet.write_with_format(0, 3, 1, &format4)?;
    worksheet.write_with_format(0, 4, 1, &format5)?;

    // Autofit the data.
    worksheet.autofit();

    workbook.save("worksheet.xlsx")?;

    Ok(())
}
