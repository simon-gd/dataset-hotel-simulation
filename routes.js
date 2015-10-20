var express = require('express');
var router = express.Router();
var tungus = require('tungus');
var mongoose = require('mongoose');
mongoose.connect('tingodb://data_db/dataset-hotel-simulation-18-hours');
var schema = require('./schema')(mongoose);
var fs = require("fs");

console.log('Running mongoose version %s', mongoose.version);

router.get('/', function(req, res) {
  res.render('index', { title: 'Dataset Instrucions' });
});

router.get('/context.html', function(req, res) {
  res.render('context', { title: 'Hotel Simulation Context' });
});

router.get('/data.json', function(req, res) {
  var data = require("./data.json");
  res.status(200).json(data);
});


router.get('/db_export', function(req, res) {
  var model = schema.model;
  //console.log(model);
  model.find(function(err, records){
    if(err){
      console.error(err);
      res.status(500).send(err);
    }else{
      if(records){
         fs.writeFile("data.json", JSON.stringify(records, null, 2), function (err) {
            res.status(200).send("Records saved");
         });
      }else{
        res.send("No Records Found");
      }
    }
  });
  
});

router.get('/schema.json', function(req, res) {
  var myschema = require("./json_table_schema.json");
  res.status(200).json(myschema);
});

router.get('/datapackage.json', function(req, res) {
  var baseUrl = req.protocol + '://' + req.get('host');
  var  datapackage = {name: "dataset-hotel-simulation",
                      title: "Dataset Hotel Simulation",
                      base: baseUrl,
                      resources: [{path: "data.json",
                                   format: "json",
                                   schema: "schema.json"}],
                      };
	res.status(200).send(JSON.stringify(datapackage, null, 2));
});

module.exports = router;
