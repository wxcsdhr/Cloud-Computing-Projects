$(function () {
    var socket    = io();
    var markers   = {};
    var polylines = {};
    var map;
    
    socket.on('update', function (msg) {
	try {
	    var newLocation = new google.maps.LatLng(msg.latitude, msg.longitude);
	    var marker = getMarker(msg.driverId);
	    
	    marker.setPosition(newLocation);
	    var polyline = getPolyline(msg.driverId);
	    
	    var path = polyline.getPath();
	    
	    path.push(newLocation);

	    if (path.length > 3) {
		path.getArray().shift();
	    }
	    
	} catch (e) {
	    console.log(e);
	}
    });
    
    socket.on('events', function (msg) {
	try {
	    
	} catch (e) {
	    console.log(e);
	}
    });
    
    function getMarker(driverId) {
	var marker = markers[driverId.toString()];

	if (!marker) {
	    marker = new google.maps.Marker({
		map: map,
		icon: "https://s3.amazonaws.com/cmucc-samza/car.png"
	    });
	    markers[driverId.toString()] = marker;
	}
	return marker;
    }
    
    function getPolyline(driverId) {
	var polyline = polylines[driverId.toString()];
	
	if (!polyline) {
	    polyline = new google.maps.Polyline({
		map: map,
		path: [],
		geodesic: true,
		strokeColor: "#CCCCCC",
		strokeOpacity: 0.0,
		strokeWeight: 2
	    });
	    polylines[driverId.toString()] = polyline;
	}

	return polyline;
    }

    function initialize() {
	map = new google.maps.Map(document.getElementById("map"), {
	    center: { lat: 40.8005552,lng: -73.9514689 },
	    zoom: 12,
	    mapTypeId: google.maps.MapTypeId.ROADMAP
	});
    }
    
    google.maps.event.addDomListener(window, 'load', initialize);
});
