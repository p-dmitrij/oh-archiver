/*
Updating of item InfluxDB tags

List of updated tags:
* influxdb::RetDate (retirement date)
* influxdb::<main value> (measurement)

OpenHab doesn't allow to set different retension policies for items, so
the retension and archiving of measurements implemented manually in 3 parts:
1. OpenHab (this script) sets the tag RetDate in the namespace "influxdb" at
   relevant items based on the group with a name like G_Ret_<N>M|Y (calls 
   retention group). An item can belong only to a single retention group.
   InfluxDB will store the current tag value in each record (point), written
   for the item by the persistency system. The tag value is calculated as
   <today> + <N months/years>
   where <N months/years> comes from the group name and means an amount of
   months (M) or years (Y), after that the point should be retired.
2. Every month the script ohdb:/root/retire_measurements.sh selects points,
   that have the tag value RetDate equals to the current date, sends them to
   the archive share nas:/ohdb_retired as zipped csv files (append-files) and 
   deletes the points in the InfluxDB. Each append-file contains points of
   a single measurement.
3. The archive server (nas) receives the append-files and appends they content
   to a corresponding measurement archive file. This logic is implemented in
   the script nas:/root/append-ohdb-retired.sh, which is called by the rsync
   server after each rsync session (option post-Xfer exec)

The script should be executed in the best case after each change of an item
definition, because the item can be moved from one retention group in another.
It seems there is no hook "on item definition changed", so currently it is
sheduled one time a day.

The "measurement" in the process described above means the value of the tag 
"_measurement", that OH automatically writes to the InfluxDB. By default it
is equal to the tag "item" (item name). The script overwrites the default 
measurement value (main value of the namespace "influxdb") as the item name 
without postfixes. If an item already has the namespace "influxdb" with a 
non-empty main value, it will be kept.
For example, the items S_UpFgl_WindDirectionRaw, S_UpFgl_WindDirection_v and
S_UpFgl_WindDirection have the same measurement value "S_UpFgl_WindDirection".
The points (records) of items with the same measurement will be stored in one
archive file. ATTENTION: all items of a measurement should have the same type!
*/

'use strict';
var myLog = Java.type("org.slf4j.LoggerFactory").getLogger("org.openhab.core.model.script.Rules");
var FrameworkUtil = Java.type("org.osgi.framework.FrameworkUtil");
var _bundle = FrameworkUtil.getBundle(scriptExtension.class);
var bundle_context = _bundle.getBundleContext();
var MetadataRegistry_Ref = bundle_context.getServiceReference("org.openhab.core.items.MetadataRegistry");
var MetadataRegistry = bundle_context.getService(MetadataRegistry_Ref);
var Metadata = Java.type("org.openhab.core.items.Metadata");
var MetadataKey = Java.type("org.openhab.core.items.MetadataKey");

// Get measurement name from the item name
function getItemBaseName(itemName) {
  var matches = /^(\w+)(_[vr]|Raw)$/.exec(itemName);
  return matches ? matches[1] || itemName : itemName;
}

// Calculated retantion date for the tag RetDate
function getRetantionDate(groupName) {
  var matches = /^G_Ret_(\d+)([M|Y])$/.exec(groupName);
  if (!matches || !matches[1] || !matches[2]) return false;
  var months = parseInt(matches[2] == "Y" ? matches[1] * 12 : matches[1]);
  var retDate = new Date();
  retDate.setMonth(retDate.getMonth() + months);
  return retDate.toISOString().slice(0, 7); // 2025-06 from 2025-06-10T00:54:26.094Z
}

// Process all available items (excluding groups)
ir.getItems().forEach(function(item) { 
  if (item.type == "Group") return;
  
  // Acquire item Influxdb metadata
  var itemInfluxdbMetaKey = new MetadataKey("influxdb", item.getName());
  var itemInfluxdbMeta = MetadataRegistry.get(itemInfluxdbMetaKey);
  
  // Get item base name (a common name for raw, virtual items, etc)
  // It is used as main metadata value (mapped in Influxdb as "_measurement")
  var itemInfluxdbMetaValue = itemInfluxdbMeta ? itemInfluxdbMeta.getValue() : "";
  if (!itemInfluxdbMetaValue) itemInfluxdbMetaValue = getItemBaseName(item.getName());
  
  // Process item retention groups
  var groupNames = item.getGroupNames();
  var isItemInRetentionGroup = false;
  for (var groupIndex = 0; groupIndex < groupNames.length; groupIndex++) {
    var retentionDate = getRetantionDate(groupNames[groupIndex]);
    // Items without retention group are ignored
    if (!retentionDate) continue;
    if (isItemInRetentionGroup) {
      myLog.warning("RETIRE: Item " + item.getName() +
        " belongs to more then one retention group! The group " + 
        groupNames[groupIndex] + " is ignored");
      continue;
    }
    myLog.info("RETIRE: Item " + item.getName() + " belongs to the retention group "
      + groupNames[groupIndex]);
    // Create item Influxdb metadata configuration with the new retantion date
    var itemInfluxdbMetaRetention = new Metadata(
        itemInfluxdbMetaKey, itemInfluxdbMetaValue, { RetDate: retentionDate });
    // Set item retention date
    if (itemInfluxdbMeta) {
        MetadataRegistry.update(itemInfluxdbMetaRetention);
        myLog.info("RETIRE: Item " + item.getName() + " metadata is updated: RetDate=" 
          + retentionDate + "; _measurement=" + itemInfluxdbMetaValue);
    } else {
      MetadataRegistry.add(itemInfluxdbMetaRetention);
      myLog.info("RETIRE: Item " + item.getName() + " metadata created: RetDate=" 
        + retentionDate + "; _measurement=" + itemInfluxdbMetaValue);
    }
    isItemInRetentionGroup = true;
  }

  // If item has no retention group, delete influxdb metadata
  if (!isItemInRetentionGroup && itemInfluxdbMeta) {
      MetadataRegistry.remove(itemInfluxdbMetaKey);
      myLog.info("RETIRE: Item " + item.getName() + " has no retention group, metadata deleted");
    }
});
