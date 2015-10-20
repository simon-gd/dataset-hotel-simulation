
var path = require('path');
var fs = require('fs');
var AdmZip = require('adm-zip');
var tungus = require('tungus');  
var async = require("async");

var mongoose = require('mongoose');

var schema = require('./schema.js')(mongoose);
var uberConfig = schema.settings;
var tables = uberConfig["tables"];
var mainTable = tables[uberConfig["main-table"]];

console.log('Running mongoose version %s', mongoose.version);
var Record = schema.model;//mongoose.model('Record');

/**
 * Connect to the local db file
 */
var dbString = (uberConfig.db.current == "tingo") ? 'tingodb://'+__dirname+'/'+uberConfig.db.tingo.path 
                                           : 'mongodb://'+ uberConfig.db.mongo.host +'/'+uberConfig.db.mongo.db; 
dbString = dbString.replace("<table-name>", mainTable.id);
mongoose.connect(dbString, function (err) {
  // if we failed to connect, abort
  if (err) throw err;

  // we connected ok
  loadData(mainTable.source);
});

//age": 45, "age_category": "adult1", "bmi": 30.288344981231, "bmi_category": "obese", "first_name": "Maja", "gender": "female", "group": "room_inner_northeast_", "height": 181.85445521212, "home_station": [1, "room_inner_northeast_bed_window_far"], "last_name": "Pedersen", "role": "guests", "weight": 100.16671556535}, {"age": 90, "age_category": "senior", "bmi": 22.563615833003, "bmi_category": "normal", "first_name": "William", "gender": "male", "group": "room_inner_northeast_", "height": 181.17259683079, "home_station": [1, "room_inner_northeast_bed_window_near"], "last_name": "Christensen", "role": "guests", "weight": 74.061706637494}, {"age": 52, "age_category": "adult1", "bmi": 19.812814722129, "bmi_category": "normal", "first_name": "Clara", "gender": "female", "group": "room_inner_northeast_", "height": 163.44808016612, "home_station": [1, "room_inner_northeast_bed_wall_far"], "last_name": "Andersen", "role": "guests", "weight": 52.930479204236}, {"age": 59, "age_category": "adult2", "bmi": 31.297634815516, "bmi_category": "obese", "first_name": "Sofie", "gender": "female", "group": "room_inner_northeast_", "height": 169.21748280406, "home_station": [1, "room_inner_northeast_bed_wall_near"], "last_name": "Kristensen", "role": "guests", "weight": 89.619389202004}, {"age": 30, "age_category": "adult1", "bmi": 31.257313760796, "bmi_category": "obese", "first_name": "Liam", "gender": "male", "group": "room_inner_northwest_", "height": 177.28880829564, "home_station": [1, "room_inner_northwest_bed_window_far"], "last_name": "Johansen", "role": "guests", "weight": 98.245867950759}, {"age": 39, "age_category": "adult1", "bmi": 25.745567186499, "bmi_category": "pre_obese", "first_name": "Laura", "gender": "female", "group": "room_inner_northwest_", "height": 173.15298212623, "home_station": [1, "room_inner_northwest_bed_window_near"], "last_name": "Kristensen", "role": "guests", "weight": 77.190244247869}, {"age": 97, "age_category": "senior", "bmi": 19.630512405774, "bmi_category": "normal", "first_name": "Oscar", "gender": "male", "group": "room_inner_northwest_", "height": 169.2419841232, "home_station": [1, "room_inner_northwest_bed_wall_far"], "last_name": "Jensen", "role": "guests", "weight": 56.227380636015}, {"age": 95, "age_category": "senior", "bmi": 21.413663136692, "bmi_category": "normal", "first_name": "William", "gender": "male", "group": "room_inner_northwest_", "height": 184.38532154534, "home_station": [1, "room_inner_northwest_bed_wall_near"], "last_name": "Christensen", "role": "guests", "weight": 72.802058014391}, {"age": 38, "age_category": "adult1", "bmi": 29.795373393963, "bmi_category": "pre_obese", "first_name": "Sofia", "gender": "female", "group": "room_northeast_", "height": 174.12078872923, "home_station": [1, "room_northeast_bed_window_far"], "last_name": "Christensen", "role": "guests", "weight": 90.33375925483}, {"age": 69, "age_category": "senior", "bmi": 24.191640980255, "bmi_category": "normal", "first_name": "Laura", "gender": "female", "group": "room_northeast_", "height": 168.35995278543, "home_station": [1, "room_northeast_bedroom_far"], "last_name": "Petersen", "role": "guests", "weight": 68.571384655553}, {"age": 28, "age_category": "adult1", "bmi": 25.572679830317, "bmi_category": "pre_obese", "first_name": "Freja", "gender": "female", "group": "room_northwest_", "height": 172.35364206263, "home_station": [1, "room_northwest_bed_window_far"], "last_name": "Andersen", "role": "guests", "weight": 75.965634817204}, {"age": 39, "age_category": "adult1", "bmi": 30.711331522568, "bmi_category": "obese", "first_name": "Liam", "gender": "male", "group": "room_northwest_", "height": 193.99987703928, "home_station": [1, "room_northwest_bed_window_near"], "last_name": "Larsen", "role": "guests", "weight": 115.58502079843}, {"age": 56, "age_category": "adult2", "bmi": 23.60486159856, "bmi_category": "normal", "first_name": "Magnus", "gender": "male", "group": "room_northwest_", "height": 192.2996515758, "home_station": [1, "room_northwest_bedroom_near"], "last_name": "Petersen", "role": "guests", "weight": 87.288785932125}, {"age": 53, "age_category": "adult1", "bmi": 28.059938352611, "bmi_category": "pre_obese", "first_name": "William", "gender": "male", "group": "room_northwest_", "height": 177.29455183584, "home_station": [1, "room_northwest_bedroom_far"], "last_name": "Larsen", "role": "guests", "weight": 88.201809080096}, {"age": 26, "age_category": "adult1", "bmi": 29.269844660787, "bmi_category": "pre_obese", "first_name": "William", "gender": "male", "group": "room_inner_northeast_", "height": 187.99320410168, "home_station": [2, "room_inner_northeast_bed_window_far"], "last_name": "Poulsen", "role": "guests", "weight": 103.44385990447}, {"age": 46, "age_category": "adult1", "bmi": 20.576143681143, "bmi_category": "normal", "first_name": "Noah", "gender": "male", "group": "room_inner_northeast_", "height": 173.89541231485, "home_station": [2, "room_inner_northeast_bed_window_near"], "last_name": "Nielsen", "role": "guests", "weight": 62.221465125372}, {"age": 63, "age_category": "adult2", "bmi": 22.596148564104, "bmi_category": "normal", "first_name": "Liam", "gender": "male", "group": "room_inner_northeast_", "height": 181.4688443855, "home_station": [2, "room_inner_northeast_bed_wall_far"], "last_name": "Kristensen", "role": "guests", "weight": 74.411244609683}, {"age": 29, "age_category": "adult1", "bmi": 30.406012146367, "bmi_category": "obese", "first_name": "Victor", "gender": "male", "group": "room_inner_northeast_", "height": 185.47226321851, "home_station": [2, "room_inner_northeast_bed_wall_near"], "last_name": "Rasmussen", "role": "guests", "weight": 104.59656144683}, {"age": 49, "age_category": "adult1", "bmi": 28.076113162633, "bmi_category": "pre_obese", "first_name": "Freja", "gender": "female", "group": "room_inner_northwest_", "height": 169.00102666286, "home_station": [2, "room_inner_northwest_bed_window_far"], "last_name": "Christiansen", "role": "guests", "weight": 80.189161081702}, {"age": 73, "age_category": "senior", "bmi": 19.029251991333, "bmi_category": "normal", "first_name": "Noah", "gender": "male", "group": "room_inner_northwest_", "height": 181.38284045688, "home_station": [2, "room_inner_northwest_bed_window_near"], "last_name": "Andersen", "role": "guests", "weight": 62.60573441895}, {"age": 27, "age_category": "adult1", "bmi": 21.313089388714, "bmi_category": "normal", "first_name": "Clara", "gender": "female", "group": "room_inner_northwest_", "height": 166.24807269109, "home_station": [2, "room_inner_northwest_bed_wall_far"], "last_name": "Pedersen", "role": "guests", "weight": 58.906015169035}, {"age": 89, "age_category": "senior", "bmi": 20.067125461592, "bmi_category": "normal", "first_name": "Freja", "gender": "female", "group": "room_inner_southwest_", "height": 167.87753188831, "home_station": [2, "room_inner_southwest_bed_window_near"], "last_name": "Jensen", "role": "guests", "weight": 56.554910212818}, {"age": 45, "age_category": "adult1", "bmi": 30.256373790704, "bmi_category": "obese", "first_name": "Lucas", "gender": "male", "group": "room_inner_southwest_", "height": 184.32048863094, "home_station": [2, "room_inner_southwest_bed_wall_far"], "last_name": "Johansen", "role": "guests", "weight": 102.79313299432}, {"age": 25, "age_category": "adult1", "bmi": 29.013489181188, "bmi_category": "pre_obese", "first_name": "Emma", "gender": "female", "group": "room_inner_southwest_", "height": 173.76320740598, "home_station": [2, "room_inner_southwest_bed_wall_near"], "last_name": "Olsen", "role": "guests", "weight": 87.602320283833}, {"age": 56, "age_category": "adult2", "bmi": 22.0879238258, "bmi_category": "normal", "first_name": "Oscar", "gender": "male", "group": "room_northeast_", "height": 187.47412590007, "home_station": [2, "room_northeast_bed_window_far"], "last_name": "Christiansen", "role": "guests", "weight": 77.631427235737}, {"age": 29, "age_category": "adult1", "bmi": 26.09317300943, "bmi_category": "pre_obese", "first_name": "Emil", "gender": "male", "group": "room_northeast_", "height": 185.59829746053, "home_station": [2, "room_northeast_bedroom_near"], "last_name": "Andersen", "role": "guests", "weight": 89.882443384108}, {"age": 40, "age_category": "adult1", "bmi": 25.858943449202, "bmi_category": "pre_obese", "first_name": "William", "gender": "male", "group": "room_northeast_", "height": 180.45379850173, "home_station": [2, "room_northeast_bedroom_far"], "last_name": "Johansen", "role": "guests", "weight": 84.205960289171}, {"age": 25, "age_category": "adult1", "bmi": 22.532074953459, "bmi_category": "normal", "first_name": "William", "gender": "male", "group": "room_northwest_", "height": 176.34524061498, "home_station": [2, "room_northwest_bedroom_far"], "last_name": "Larsen", "role": "guests", "weight": 70.069444295037}, {"age": 88, "age_category": "senior", "bmi": 22.636219367046, "bmi_category": "normal", "first_name": "Oliver", "gender": "male", "group": "room_southeast_", "height": 185.10592534694, "home_station": [2, "room_southeast_bedroom_near"], "last_name": "Rasmussen", "role": "guests", "weight": 77.561202909386}, {"age": 53, "age_category": "adult1", "bmi": 18.738044373913, "bmi_category": "normal", "first_name": "Liam", "gender": "male", "group": "room_southeast_", "height": 171.48572571409, "home_station": [2, "room_southeast_bedroom_far"], "last_name": "Madsen", "role": "guests", "weight": 55.103630648905}, {"age": 36, "age_category": "adult1", "bmi": 22.427930539872, "bmi_category": "normal", "first_name": "Victor", "gender": "male", "group": "room_southwest_", "height": 174.58879761829, "home_station": [2, "room_southwest_bed_window_near"], "last_name": "Pedersen", "role": "guests", "weight": 68.363131860484}, {"age": 45, "age_category": "adult1", "bmi": 18.255592516861, "bmi_category": "underweight", "first_name": "Clara", "gender": "female", "group": "room_southwest_", "height": 173.16120799669, "home_station": [2, "room_southwest_bedroom_near"], "last_name": "Kristensen", "role": "guests", "weight": 54.739036269811}, {"age": 43, "age_category": "adult1", "bmi": 28.55098117008, "bmi_category": "pre_obese", "first_name": "Victor", "gender": "male", "group": "kitchen_workers", "height": 171.1131090505, "home_station": [1, "kitchen_northwest"], "last_name": "Andersen", "role": "workers", "weight": 83.596405170068}, {"age": 79, "age_category": "senior", "bmi": 23.269020661031, "bmi_category": "normal", "first_name": "Josefine", "gender": "female", "group": "kitchen_workers", "height": 156.44457967316, "home_station": [1, "kitchen_northeast"], "last_name": "Christensen", "role": "workers", "weight": 56.950710523734}, {"age": 62, "age_category": "adult2", "bmi": 21.864757225257, "bmi_category": "normal", "first_name": "Freja", "gender": "female", "group": "kitchen_workers", "height": 169.19880934674, "home_station": [1, "kitchen_southwest"], "last_name": "Andersen", "role": "workers", "weight": 62.594945363651}, {"age": 72, "age_category": "senior", "bmi": 18.992553483688, "bmi_category": "normal", "first_name": "Emil", "gender": "male", "group": "office_workers", "height": 182.97362878537, "home_station": [1, "office_northwest"], "last_name": "Rasmussen", "role": "workers", "weight": 63.585832326962}, {"age": 34, "age_category": "adult1", "bmi": 25.993530075991, "bmi_category": "pre_obese", "first_name": "William", "gender": "male", "group": "office_workers", "height": 177.96468112816, "home_station": [1, "office_northeast"], "last_name": "Larsen", "role": "workers", "weight": 82.325220922459}, {"age": 61, "age_category": "adult2", "bmi": 27.654805139317, "bmi_category": "pre_obese", "first_name": "Victor", "gender": "male", "group": "office_workers", "height": 180.94786174442, "home_station": [1, "office_southwest"], "last_name": "Madsen", "role": "workers", "weight": 90.547718821187}, {"age": 58, "age_category": "adult2", "bmi": 20.592410046693, "bmi_category": "normal", "first_name": "Emma", "gender": "female", "group": "office_workers", "height": 163.74163842603, "home_station": [1, "office_southeast"], "last_name": "Christiansen", "role": "workers", "weight": 55.210978088307}, {"age": 38, "age_category": "adult1", "bmi": 29.711600085452, "bmi_category": "pre_obese", "first_name": "Isabella", "gender": "female", "group": "office_workers", "height": 161.27253704253, "home_station": [1, "office_west"], "last_name": "Thomsen", "role": "workers", "weight": 77.276399142727}, {"age": 66, "age_category": "senior", "bmi": 22.481688894314, "bmi_category": "normal", "first_name": "Magnus", "gender": "male", "group": "reception_workers", "height": 174.66548262571, "home_station": [1, "reception_desk_west"], "last_name": "Larsen", "role": "workers", "weight": 68.587205769297}, {"age": 25, "age_category": "adult1", "bmi": 31.4550737022, "bmi_category": "obese", "first_name": "Clara", "gender": "female", "group": "reception_workers", "height": 163.85237356562, "home_station": [1, "reception_desk_center"], "last_name": "Johansen", "role": "workers", "weight": 84.449324688993}, {"age": 60, "age_category": "adult2", "bmi": 31.468916898099, "bmi_category": "obese", "first_name": "Laura", "gender": "female", "group": "reception_workers", "height": 177.26986236059, "home_station": [1, "reception_desk_east"], "last_name": "Rasmussen", "role": "workers", "weight": 98.889825502078}, {"age": 25, "age_category": "adult1", "bmi": 25.584276863918, "bmi_category": "pre_obese", "first_name": "Ida", "gender": "female", "group": "restaurant_workers", "height": 175.12633148257, "home_station": [1, "restaurant_host"], "last_name": "Pedersen", "role": "workers", "weight": 78.465012214279}, {"age": 63, "age_category": "adult2", "bmi": 30.476985992004, "bmi_category": "obese", "first_name": "Sofie", "gender": "female", "group": "restaurant_workers", "height": 170.3909438089, "home_station": [1, "restaurant_server_west"], "last_name": "Johansen", "role": "workers", "weight": 88.484058143768}, {"age": 62, "age_category": "adult2", "bmi": 23.453306680502, "bmi_category": "normal", "first_name": "Laura", "gender": "female", "group": "restaurant_workers", "height": 157.54272364891, "home_station": [1, "restaurant_server_center"], "last_name": "Petersen", "role": "workers", "weight": 58.21042650675}, {"age": 64, "age_category": "adult2", "bmi": 27.783288064211, "bmi_category": "pre_obese", "first_name": "Isabella", "gender": "female", "group": "restaurant_workers", "height": 160.90846591863, "home_station": [1, "restaurant_server_east"], "last_name": "Olsen", "role": "workers", "weight": 71.93519587787}]},

function createRecord (rawDataItem, done) {
  //console.log("createRecord");

  Record.create({
    id: rawDataItem.id
    , first_name: rawDataItem.first_name
    , last_name: rawDataItem.last_name
    , gender: rawDataItem.gender
    , age: rawDataItem.age
    , age_category: rawDataItem.age_category
    , height: rawDataItem.height
    , weight: rawDataItem.weight
    , bmi: rawDataItem.bmi
    , bmi_category: rawDataItem.bmi_category
    , group: rawDataItem.group
    , home_station: rawDataItem.home_station
    , role: rawDataItem.role
    , positions: rawDataItem.positions
    , positionsContextMapped: rawDataItem.positionsContextMapped
    , energy_consumption: rawDataItem.energy_consumption
    , water_consumption: rawDataItem.water_consumption
    , occupant_temperatures: rawDataItem.occupant_temperatures
    , window_opening: rawDataItem.window_opening
  }, function (err, rec) {
    
    if (err){ 
      console.error("error: ", err, rec);
      done(err);
    } else {  
        console.log("createRecord: ", rec.id); 
        done();   
    }
    });
      
}
// XXX this is super hacky...
 function positionsMappedToContext(floor, x, y) {
     var BIMScale = 200;
     var imageW =  860;
     var imageH = 860;
     var WRatio = imageW/BIMScale;
     var HRatio = imageH/BIMScale;
     var Foffset = 5+ ((750) * (floor-1));
     var Hoffset = -210;
     return [Foffset+(x*WRatio), imageH-(y*HRatio)+Hoffset]; 
};

function processJSONfile(data){
   var demographics = data["StatisticValues"]["Demographics"];
   var people = {};
   var i,j;
   for (i=1; i <= demographics.length; i++){
      people[i]=demographics[i-1];
      people[i]["id"] = i;
      //console.log("demographics ", i);
   }
   var messageGroups = data["PostSimulationMessageSeries"];
   for(i=0; i < messageGroups.length; i++){
      var messageGroup = messageGroups[i];
      var time = messageGroup["Time"];
      for(j=0; j < messageGroup["Messages"].length; j++){
        var message = messageGroup["Messages"][j];
        switch(message["Port"]) {
            case "Consumption Rate":
              if(message["Value"][1] === "power"){
                var id = message["Value"][0];

                if(!people[id]["energy_consumption"]){
                  people[id]["energy_consumption"] = {time:[], series:[], total: 0};
                }
                people[id]["energy_consumption"].time.push(time);
                people[id]["energy_consumption"].series.push(message["Value"][2]);
                //people[id]["energy_consumption"].count = people[id]["energy_consumption"].count + 1;


              }else if(message["Value"][1] === "water"){
                var id = message["Value"][0];

                if(!people[id]["water_consumption"]){
                  people[id]["water_consumption"] = {time:[], series:[], total: 0};
                }
                people[id]["water_consumption"].time.push(time);
                people[id]["water_consumption"].series.push(message["Value"][2]);
                //people[id]["water_consumption"].count = people[id]["water_consumption"].count + 1;  
              }
              break;
            case "Occupant Location Change":
              var id = message["Value"]["id"];
              //console.log("positions ", id, people[id]);
              if(!people[id]["positions"]){
                people[id]["positions"] = {time:[], floor:[], x:[], y:[]};
              }
              if(!people[id]["positionsContextMapped"]){
                people[id]["positionsContextMapped"] = {time:[], x:[], y:[]};
              }
              people[id]["positions"].time.push(time);
              people[id]["positions"].floor.push(message["Value"]["value"][0]);
              people[id]["positions"].x.push(message["Value"]["value"][1]);
              people[id]["positions"].y.push(message["Value"]["value"][2]);

              var remappedPt = positionsMappedToContext(message["Value"]["value"][0], message["Value"]["value"][1], message["Value"]["value"][2]);
              people[id]["positionsContextMapped"].time.push(time);
              people[id]["positionsContextMapped"].x.push(remappedPt[0]);
              people[id]["positionsContextMapped"].y.push(remappedPt[1]);

              //people[id]["positions"].count = people[id]["positions"].count + 1;  
              break;
            case "Occupant Temperature Change":
              var id = message["Value"][0];

              if(!people[id]["occupant_temperatures"]){
                  people[id]["occupant_temperatures"] = {time:[], series:[]};
              }
              people[id]["occupant_temperatures"].time.push(time);
              people[id]["occupant_temperatures"].series.push(message["Value"][1]);
              //people[id]["occupant_temperatures"].count = people[id]["occupant_temperatures"].count + 1;  
              break;
            case "Window Position Change":
              var id = message["Value"][3];
              var floor = message["Value"][0];
              var windowid = message["Value"][1];
              var state = message["Value"][2];
              if(!people[id]["window_opening"]){
                  people[id]["window_opening"] = {time:[], series:[]};
              }
              people[id]["window_opening"].time.push(time);
              people[id]["window_opening"].series.push(1); //[message["Value"][0], message["Value"][1], message["Value"][2]]);
              //people[id]["window_opening"].count = people[id]["window_opening"].count + 1;  

              break;
            default:
              break;
        }

      }
   }

   // Calculate total consumption
   for(p in people){
    people[p]["positions"].count = people[p]["positions"].time.length;
    if(people[p]["window_opening"]){
      people[p]["window_opening"].count = people[p]["window_opening"].time.length;
    }
    people[p]["positionsContextMapped"].count = people[p]["positionsContextMapped"].time.length;
    
    // water_consumption
    var total = 0;
    var water_consumption = people[p]["water_consumption"];
    for(var t = 1; t < water_consumption.time.length; t++){
      var t0 = water_consumption.time[t-1];
      var t1 = water_consumption.time[t];
      var timeMS = t1-t0;
      var timeMin = timeMS / 60000.0;
      var rate = water_consumption.series[t-1];  // L/min
      total += rate*timeMin;
    }
    people[p]["water_consumption"]["total"] = total;

    // energy_consumption
    total = 0;
    var energy_consumption = people[p]["energy_consumption"];
    for(var t = 1; t < energy_consumption.time.length; t++){
      var t0 = energy_consumption.time[t-1];
      var t1 = energy_consumption.time[t];
      var timeMS = t1-t0;
      var timeHour = timeMS / 3600000.0;
      var rate = energy_consumption.series[t-1];  // W
      total += rate*timeHour;
    }
    people[p]["energy_consumption"]["total"] = total;


     // energy_consumption
    var total = 0;
    var totalTime = 0; 
    var occupant_temperatures = people[p]["occupant_temperatures"];
    for(var t = 1; t < occupant_temperatures.time.length; t++){
      var t0 = occupant_temperatures.time[t-1];
      var t1 = occupant_temperatures.time[t];
      var timeMS = t1-t0;
      var timeHour = timeMS / 3600000.0;
      var temp = occupant_temperatures.series[t-1];  // W
      total += temp*timeHour;
      totalTime += timeHour;
    }
    people[p]["occupant_temperatures"]["avg"] = total / totalTime;

   }


   var pArr = [];
   for(p in people){
    pArr.push(people[p]);
   }


   async.each(pArr, function(k, callback){  
        createRecord(k, function(err){
          if(err){ console.error("createData", err); }
         callback();  
        });
   }, function(err){
        // wer are done with all records
        finaldone(err);
   });
}

function loadData (filepath) {
  console.log("createData");
  var stat = fs.statSync(filepath);
  var ext = path.extname(filepath).substr(1);
  
  if(ext === "zip"){
    var zip = new AdmZip(filepath);
      var zipEntries = zip.getEntries(); // an array of ZipEntry records
      if(zipEntries.length > 1){
        console.warning("dataStore: Warning: Currently support zips with one file.");
      }
      var zipEntry = zipEntries[0];
      var uncompressedData = zip.readAsText(zipEntry);
      var data = JSON.parse(uncompressedData);
      processJSONfile(data);

  }else if(ext === "json"){
    fs.readFile(inputDatapath, 'utf8', function (err, datain) {
      if (err) {
        finaldone(err);
      }
      var data = JSON.parse(datain);
      processJSONfile(data);
    });//fs.readFile
  }else{
    console.warning("dataStore: Warning: Currently only support zipped json files as externalfile.");
  }


}


function finaldone (err) {
  if (err){
   console.error(err);
  } else{

    mongoose.disconnect();

  }


  
}