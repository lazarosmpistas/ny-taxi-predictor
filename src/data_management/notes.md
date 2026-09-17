- i do not think we need store_and_fwd_flag
- fares, payment types etc are useless because they are calculated at the end of a trip, we need to predict the trip 
before the actual trip
- RatecodeID None should be renamed to "99"
- possibly negative fares are actually positive with a minus by a bug
- many NAs on key columns / find correlation and predict them?