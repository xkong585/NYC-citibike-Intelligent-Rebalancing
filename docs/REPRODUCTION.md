# Local reproduction record

The January 2023 archive was downloaded from the original repository's LFS content endpoint and verified against SHA-256 7a3a25d977e72f46307d52cab8a450ceb0f496533c6e9cabfd4bc3c4655c8849. Both CSV members were processed.

## Changes and checks

- Added a pandas workflow requiring no Databricks cluster or DBFS paths.
- Outer-joined arrivals and departures instead of retaining only departure-observed station-hours.
- Checked aggregate arrivals and departures against the cleaned trip count before calendar clipping.
- Used complete hourly calendars for forecast comparisons.
- Selected stations using training-period volume only; fit hour-of-week means on training dates only.
- Recorded MAE/RMSE and explained the rolling previous-day comparator.
- Regenerated the README figures with indigo/teal/amber styling and explicit units.

## Scope

One source month was executed; the full 12-month Databricks pipeline was not. The four later DBC notebooks contain blank command cells. No actual station capacities, initial bike inventories, dispatch costs, or vehicle-routing constraints were available, so no operational improvement or completed VRP result is claimed. A trip archive cannot by itself identify stockouts or lost demand. Original files and documentation remain in the repository for provenance.
