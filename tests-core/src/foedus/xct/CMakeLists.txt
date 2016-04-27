add_foedus_test_individual(test_retrospective_lock_list "CllAddSearch;CllBatchInsertFromEmpty;CllBatchInsertMerge;CllReleaseAfterSimple;CllReleaseAfterExtended")


set(test_sysxct_lock_list_individuals
  InOrderRecordSimple
  InOrderRecordExtended
  ReverseOrderRecordSimple
  ReverseOrderRecordExtended
  IsTryRequiredRecord
  InOrderMixedSimple
  InOrderMixedExtended
  ReverseOrderMixedSimple
  ReverseOrderMixedExtended
  UnconditionalLivelockSimple
  UnconditionalLivelockExtended
  TryLivelockSimple
  TryLivelockExtended
  TryDeadlockSimple
  TryDeadlockExtended
)
add_foedus_test_individual(test_sysxct_lock_list "${test_sysxct_lock_list_individuals}")

add_foedus_test_individual(test_xct_access "CompareReadSet;SortReadSet;RandomReadSet;CompareWriteSet;SortWriteSet;RandomWriteSet")
add_foedus_test_individual(test_xct_commit_conflict "NoConflict;LightConflict;HeavyConflict;ExtremeConflict")
add_foedus_test_individual(test_xct_id "Empty;SetAll;SetEpoch;SetOrdinal;SetThread")

set(test_xct_mcs_impl_individuals
  InstantiateSimple
  InstantiateExtended
  NoConflictSimple
  NoConflictExtended
  ConflictSimple
  ConflictExtended
  RandomSimple
  RandomExtended
  NonCanonical1Simple
  NonCanonical1Extended
  NonCanonical2Simple
  NonCanonical2Extended
  AsyncReadOnlySimple
  AsyncReadOnlyExtended
  AsyncWriteOnlySimple
  AsyncWriteOnlyExtended
  AsyncReadWriteSimple
  AsyncReadWriteExtended
)
add_foedus_test_individual(test_xct_mcs_impl "${test_xct_mcs_impl_individuals}")
add_foedus_test_individual(test_xct_mcs_impl_ww "Instantiate;NoConflict;Conflict;Initial;Random")
