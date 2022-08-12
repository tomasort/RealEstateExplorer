
function numberWithCommas(x) {
  return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function titleCase(str) {
  var splitStr = str.toLowerCase().split(' ');
  for (var i = 0; i < splitStr.length; i++) {
    // You do not need to check if i is larger than splitStr length, as your for does that for you
    // Assign it back to the array
    splitStr[i] = splitStr[i].charAt(0).toUpperCase() + splitStr[i].substring(1);
  }
  // Directly return the joined string
  return splitStr.join(' ');
}
//
//
// Show properties in a table
//
//
function showProperties(properties, table) {
  for (var i = 0; i < properties.length; i++) {
    var currentProperty = properties[i];
    var newRow = table.insertRow();
    // Loop through the keys of each property but exclude longitude and latitude
    for (var j = 0; j < Object.keys(currentProperty).length - 2; j++) {
      var value = Object.values(currentProperty)[j]
      if (['latitude', 'longitude', 'geo_point'].includes(Object.keys(currentProperty)[j])) {
        continue;
      }
      var newCell = newRow.insertCell(j);
      if (!['date_sold', 'detail_url', 'address', 'distance', 'zip'].includes(Object.keys(currentProperty)[j])) {
        value = numberWithCommas(value);
      }
      if ('detail_url' == (Object.keys(currentProperty)[j])) {
        var a = document.createElement('a');
        a.appendChild(document.createTextNode(value.slice(0, 22) + "..."));
        a.href = value
        newCell.appendChild(a)
        continue
      }
      var newText = document.createTextNode(value);
      newCell.appendChild(newText);
    }
  }
}

var tableRef = document.getElementById('results');
showProperties(properties, tableRef);
//
//
// Show the statistical results
//
//
var details = document.getElementById('details').getElementsByTagName('ul')[0];
for (var i = 0; i < Object.keys(stats).length; i++) {
  var label = titleCase(Object.keys(stats)[i].replace(/_/g, ' '));
  var value = Number(Object.values(stats)[i]).toFixed(1);
  var newDetailItem = document.createElement("li");
  newDetailItem.appendChild(document.createTextNode(label + ": " + numberWithCommas(value)));
  details.appendChild(newDetailItem);
}
var target_details = document.getElementById('target_details').getElementsByTagName('ul')[0];
for (var i = 0; i < Object.keys(targetInfo).length; i++) {
  var value = Object.values(targetInfo)[i];
  var newDetailItem = document.createElement("li");
  if (['price', 'area'].includes(Object.keys(targetInfo)[i])) {
    newDetailItem.appendChild(document.createTextNode("Target " + Object.keys(targetInfo)[i] + ": " + value));
  } else {
    newDetailItem.appendChild(document.createTextNode(value));
  }
  target_details.appendChild(newDetailItem);
}
//
//
// Show the map
//
//
var map;
function initMap() {
  var map = new google.maps.Map(document.getElementById('map'), {
    center: mainProperty,
    zoom: 15
  });
  var marker = new google.maps.Marker({ position: mainProperty, map: map });
  for (var i = 0; i < properties.length; i++) {
    if (properties[i].status == 'FOR_SALE') {
      var newMarker = new google.maps.Marker({ position: { lat: properties[i].latitude, lng: properties[i].longitude }, icon: green_icon, map: map });
    } else {
      var newMarker = new google.maps.Marker({ position: { lat: properties[i].latitude, lng: properties[i].longitude }, icon: red_icon, map: map });
    }
  }
  var myCircle = new google.maps.Circle({
    map: map,
    center: mainProperty,
    radius: radius
  });
}

