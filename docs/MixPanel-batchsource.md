# MixPanel batch source

Description
-----------
This plugin used to fetch MixPanel events.

Properties
----------
### General

**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**API Secret:** API secret used to authenticate in MixPanel API.

**From date:** Start date for reports data in yyyy-mm-dd format.

**To date:** End date for reports data in yyyy-mm-dd format.

### Advanced

**Generate schema by events:** If enabled, schema will include all unique fields from selected events. Missing
fields for particular event will have null values. Fields names will be escaped to match Apache Avro naming conventions.

**Events:** Comma separated list of events you would like to get data on.

**Filter:** Expression to filter events by(see MixPanel [documentation](https://developer.mixpanel.com/docs/data-export-api#section-segmentation-expressions) for reference).
