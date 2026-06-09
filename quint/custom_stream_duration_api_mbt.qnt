/// Compact MBT surface for public custom stream duration request admission.
module custom_stream_duration_api_mbt {
  type ApiCase = {
    operation: str,
    tableExtension: bool,
    defaultItemExtension: bool,
    putTtl: bool,
    updateTtl: bool,
    conflictingTxnTtl: bool,
  }

  var lastCase: ApiCase
  var lastDecision: str

  pure def initialCase = {
    operation: "update_table",
    tableExtension: false,
    defaultItemExtension: false,
    putTtl: false,
    updateTtl: false,
    conflictingTxnTtl: false,
  }

  pure def expectedDecision(apiCase) =
    if (apiCase.operation == "update_table")
      if (apiCase.tableExtension or apiCase.defaultItemExtension) "extension_present" else "standard"
    else if (apiCase.operation == "batch_write")
      if (apiCase.putTtl) "extension_present" else "standard"
    else if (apiCase.putTtl and apiCase.updateTtl and apiCase.conflictingTxnTtl)
      "conflicting_ttl_rejected"
    else if (apiCase.putTtl or apiCase.updateTtl)
      "extension_present"
    else
      "standard"

  action init = all {
    lastCase' = initialCase,
    lastDecision' = "not_checked",
  }

  action Check = {
    nondet operation = Set("update_table", "batch_write", "transact_write").oneOf()
    nondet tableExtension = Set(false, true).oneOf()
    nondet defaultItemExtension = Set(false, true).oneOf()
    nondet putTtl = Set(false, true).oneOf()
    nondet updateTtl = Set(false, true).oneOf()
    nondet conflictingTxnTtl = Set(false, true).oneOf()
    val apiCase = {
      operation: operation,
      tableExtension: tableExtension,
      defaultItemExtension: defaultItemExtension,
      putTtl: putTtl,
      updateTtl: updateTtl,
      conflictingTxnTtl: conflictingTxnTtl,
    }
    all {
      lastCase' = apiCase,
      lastDecision' = expectedDecision(apiCase),
    }
  }

  action step = Check

  val inv =
    all {
      lastDecision != "conflicting_ttl_rejected" or lastCase.operation == "transact_write",
      lastDecision != "conflicting_ttl_rejected" or lastCase.putTtl and lastCase.updateTtl and lastCase.conflictingTxnTtl,
      lastDecision != "extension_present" or lastCase.operation != "update_table" or lastCase.tableExtension or lastCase.defaultItemExtension,
      lastDecision != "extension_present" or lastCase.operation != "batch_write" or lastCase.putTtl,
    }

  run standardUpdateTableIsStandard =
    true.expect(expectedDecision(initialCase) == "standard")

  run tableDurationExtensionIsDetected =
    true.expect(expectedDecision({
      operation: "update_table",
      tableExtension: true,
      defaultItemExtension: false,
      putTtl: false,
      updateTtl: false,
      conflictingTxnTtl: false,
    }) == "extension_present")

  run batchPutTtlExtensionIsDetected =
    true.expect(expectedDecision({
      operation: "batch_write",
      tableExtension: false,
      defaultItemExtension: false,
      putTtl: true,
      updateTtl: false,
      conflictingTxnTtl: false,
    }) == "extension_present")

  run conflictingTransactionTtlRejects =
    true.expect(expectedDecision({
      operation: "transact_write",
      tableExtension: false,
      defaultItemExtension: false,
      putTtl: true,
      updateTtl: true,
      conflictingTxnTtl: true,
    }) == "conflicting_ttl_rejected")
}
