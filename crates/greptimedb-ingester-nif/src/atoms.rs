rustler::atoms! {
    ok,
    error,
    // Options
    endpoints,
    dbname,
    username,
    password,
    ts_column,
    ttl,
    tls,
    verify,
    verify_peer,
    verify_none,
    ca_cert,
    client_cert,
    client_key,
    cipher_suites,

    // FIPS status
    fips_enabled,

    // Rows
    fields,
    tags,
    timestamp,
    ts,
}
