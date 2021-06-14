extern "C" {
    fn fluvio_filter(ptr: *const u8, len: usize) -> bool;
}

mod __system {
    // use fluvio_smartstream::dataplane::core::Encoder;

    #[no_mangle]
    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn filter(ptr: *mut u8, len: usize) -> i32 {
        extern "C" {
            fn copy_records(putr: i32, len: i32);
        }

        let input_data = Vec::from_raw_parts(ptr, len, len);
        let mut records: Vec<fluvio_smartstream::dataplane::record::Record> = vec![];
        if let Err(_err) = fluvio_smartstream::dataplane::core::Decoder::decode(
            &mut records,
            &mut std::io::Cursor::new(input_data),
            0,
        ) {
            return -1;
        };

        let mut processed: Vec<_> = records
            .into_iter()
            .filter(|record| {
                super::fluvio_filter(
                    record.value.as_ref()[0] as *const u8,
                    record.value.as_ref().len(),
                )
            })
            .collect();

        let mut out = vec![];
        if let Err(_) =
            fluvio_smartstream::dataplane::core::Encoder::encode(&mut processed, &mut out, 0)
        {
            return -1;
        }

        let out_len = out.len();
        let ptr = out.as_mut_ptr();
        std::mem::forget(out);

        copy_records(ptr as i32, out_len as i32);

        processed.len() as i32
    }
}
