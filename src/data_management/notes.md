## what data_norm does right now
- RatecodeID None is being renamed to "99"
## issues to be tackled
- store_and_fwd_flag can be used to detect suspicious trips when Y (no internet in taxi, trip uploaded later)
- possibly negative fares are actually positive with a minus by a bug
- many NAs on key columns / find correlation and predict them?
- total_amount is inconsistent when everything is added to roughly 1/4 of rows