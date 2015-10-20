
/**
 * Record schema
 */
module.exports = function(mongoose) {
  var Schema = mongoose.Schema;

  // Actual data that gets stored in DB
  var recordSchema = new Schema({
      id: Number
    , first_name: String
    , last_name: String
    , gender: String
    , age: Number
    , age_category: String
    , height: Number
    , weight: Number
    , bmi: Number
    , bmi_category: String
    , group: String
    , home_station: Schema.Types.Mixed
    , role: String
    , positions: { 
                  count: Number,
                  time: [Number]
                  , floor: [Number]
                  , x: [Number]
                  , y: [Number]
      }
    , positionsContextMapped: {
                  count: Number, 
                  time: [Number]
                  , x: [Number]
                  , y: [Number]
      }
    , energy_consumption: {
                  time: [Number]
                  , total: Number
                  , series: [Number]
      }
    , water_consumption: {
                  time: [Number]
                  , total: Number
                  , series: [Number]
      }
    , occupant_temperatures: { 
                  time: [Number]
                  , avg: Number
                  , series: [Number]
      }
    , window_opening: { 
                  count: Number
                  , time: [Number]
                  , series: [Schema.Types.Mixed]
      }
  });
  
  recordSchema.statics.fieldMeta = function () {
    return [{"id":"id", "name":"ID", "dtype": "uint32"}, 
            {"id":"first_name", "name":"First Name", "dtype": "string"}, 
            {"id":"last_name", "name":"Last Name", "dtype": "string"}, 
            {"id":"gender", "name":"Gender", "dtype": "string"}, 
            {"id":"age", "name":"Age", "dtype": "uint16", "aggregate": "avg"},
            {"id":"age_category", "name":"Age Category", "dtype": "uint16"}, 
            {"id":"height", "name":"Height", "dtype": "uint16", "aggregate": "avg"},
            {"id":"weight", "name":"Weight", "dtype": "uint16", "aggregate": "avg"}, 
            {"id":"bmi", "name":"BMI", "dtype": "float32", "aggregate": "avg"}, 
            {"id":"bmi_category", "name":"BMI Category", "dtype": "string"}, 
            {"id":"group", "name":"Group", "dtype": "string"}, 
            {"id":"role", "name":"Role", "dtype": "string"}, 
            {"id":"positionsFloor", 
             "name":"Floor", 
             "color": "#1f77b4", 
             "dtype": "timeseries", 
             "display-dtype": "int32",
             "time-dtype": "int32",
             "time-scale": "ms",
             "series-type": "continuous",
             "series-dtype": "int32",
             "series-units": "position",
             "filter":"interp"},
            {"id":"positionsX", 
             "name":"X", 
             "color": "#aec7e8",
             "dtype": "timeseries", 
             "display-dtype": "int32",
             "time-dtype": "int32",
             "time-scale": "ms",
             "series-type": "continuous",
             "series-dtype": "int32",
             "series-units": "position",
             "filter":"interp"},
            {"id":"positionsY", 
             "name":"Y", 
             "color": "#ff7f0e",  
             "dtype": "timeseries", 
             "display-dtype": "int32",
             "time-dtype": "int32",
             "time-scale": "ms",
             "series-type": "continuous",
             "series-dtype": "int32",
             "series-units": "position",
             "filter":"interp"},
            {"id":"energy_consumption_d", 
             "name":"Energy", 
             "color": "#ffbb78",  
             "dtype": "timeseries", 
             "display-dtype": "int32",
             "display-units": "Wh",
             "time-dtype": "int32",
             "time-scale": "ms",
             "series-type": "continuous-rate",
             "series-dtype": "int32",
             "series-units": "W",
             "filter":"interp2",
             "aggregate": "avg"},
             {"id":"water_consumption_d", 
             "name":"Water", 
             "color": "#2ca02c",  
             "dtype": "timeseries", 
             "display-dtype": "int32",
             "display-units": "L",
             "time-dtype": "int32",
             "time-scale": "ms",
             "series-type": "continuous-rate",
             "series-dtype": "int32",
             "series-units": "L/min",
             "filter":"interp2",
             "aggregate": "avg"},
            {"id":"occupant_temperatures_d", 
             "name":"Temperature", 
             "color": "#98df8a",  
             "dtype": "timeseries", 
             "display-dtype": "int32",
             "time-dtype": "int32",
             "time-scale": "ms",
             "series-type": "continuous",
             "series-dtype": "int32",
             "series-units": "ºC",
             "filter":"interp",
             "aggregate": "avg"},
             {"id":"window_opening_d", 
             "name":"Windows", 
             "color": "#d62728",  
             "dtype": "timeseries", 
             "display-dtype": "int32",
             "time-dtype": "int32",
             "time-scale": "ms",
             "series-type": "event-simple",
             "series-dtype": "int32",
             "series-units": "open",
             "filter":"hist",
             "aggregate": "sum"}
           ];
  };
  // Virtual Schema data that is not stored per object, and is shared for all objects
  
  
  recordSchema.virtual('positionsFloor').get(function () {
     return this.positions.time.length; 
  });
  recordSchema.virtual('positionsFloor_series').get(function () {
    return this.positions.floor;
  });
  recordSchema.virtual('positionsFloor_time').get(function () {
    return this.positions.time;
  });
  recordSchema.virtual('positionsX').get(function () {
     return this.positions.time.length; 
  });
  recordSchema.virtual('positionsX_series').get(function () {
    return this.positions.x;
  });
  recordSchema.virtual('positionsX_time').get(function () {
    return this.positions.time;
  });
  recordSchema.virtual('positionsY').get(function () {
     return this.positions.time.length; 
  });
  recordSchema.virtual('positionsY_series').get(function () {
    return this.positions.y;
  });
  recordSchema.virtual('positionsY_time').get(function () {
    return this.positions.time;
  });

  // energy_consumption
  recordSchema.virtual('energy_consumption_d').get(function () {
     return this.energy_consumption.total; 
  });
  recordSchema.virtual('energy_consumption_d_series').get(function () {
    return this.energy_consumption.series;
  });
  recordSchema.virtual('energy_consumption_d_time').get(function () {
    return this.energy_consumption.time;
  });

   // water_consumption
  recordSchema.virtual('water_consumption_d').get(function () {
     return this.water_consumption.total; 
  });
  recordSchema.virtual('water_consumption_d_series').get(function () {
    return this.water_consumption.series;
  });
  recordSchema.virtual('water_consumption_d_time').get(function () {
    return this.water_consumption.time;
  });

   // occupant_temperatures
  recordSchema.virtual('occupant_temperatures_d').get(function () {
     return this.occupant_temperatures.avg; 
  });
  recordSchema.virtual('occupant_temperatures_d_series').get(function () {
    return this.occupant_temperatures.series;
  });
  recordSchema.virtual('occupant_temperatures_d_time').get(function () {
    return this.occupant_temperatures.time;
  });


  // window_opening
  recordSchema.virtual('window_opening_d').get(function () {
     return this.window_opening.time.length; 
  });
  recordSchema.virtual('window_opening_d_series').get(function () {
    return this.window_opening.series;
  });
  recordSchema.virtual('window_opening_d_time').get(function () {
    return this.window_opening.time;
  });

  var Record = mongoose.model("Record", recordSchema);

  var c8_settings = {
    "db": {
      "current": "tingo",
      "mongo": {
        "host": "localhost",
        "port": 27017,
        "db": "dataset-hotel-simulation-18-hours",
        "opts": {
          "auto_reconnect": true,
          "safe": true
        }
      },
      "tingo": {
        "path": "data_db/dataset-hotel-simulation-18-hours"
      }
    },
    "main-table": 0,
    "tables":[
      {"id": "dataset-hotel-simulation-18-hours",
       "name": "18 Hour Results",
       "source": "./data_raw/Hotel-Simulation-18-hours.zip",
       "schema":  "schema.js" 
      },
      {"id": "dataset-hotel-simulation-72-hours",
       "name": "72 Hour Results",
       "source": "./data_raw/Hotel-Simulation-72-hours.zip",
       "schema":  "schema.js" 
      } 
    ]
  };

  return {schema: recordSchema, model: Record, settings: c8_settings};
};