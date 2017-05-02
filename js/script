function httpRequest(address, reqType, action) {
    var r = new XMLHttpRequest();
    r.open(reqType, address, true);
    r.onload = function (e) {
        if (r.readyState == 4 && r.status == 200) {
            action(JSON.parse(r.responseText));
        }
    };
    r.send();
}
function on_request() {
    let list1 = document.getElementById("list1");
    let list2 = document.getElementById("list2");

    httpRequest("/path?from=" + encodeURIComponent(list1.options[list1.selectedIndex].value) + "&to=" + encodeURIComponent(list2.options[list2.selectedIndex].value), "POST", function(response) {
        var nodes = response.nodes;
        /*[
         {id: 1, label: 'Fixed node', x:0, y:0, fixed:true},
         {id: 2, label: 'Drag me', x:150, y:130, physics:false},
         {id: 3, label: 'Obstacle', x:80, y:-80, fixed:true, mass:10}
         ];*/
        console.log(response);
        var edges = response.edges;
        console.log(edges);
        // create an array with edges
        /*var edges = [
         {from: 1, to: 2, arrows:'to'}
         ];*/
        // create a network
        var container = document.getElementById('mynetwork');
        var data = {
            nodes: nodes,
            edges: edges
        };
        var options = {
            physics:true,
            configure:function (option, path) {
                if (path.indexOf('smooth') !== -1 || option === 'smooth') {
                    return true;
                }
                return false;
            },
            edges: {
                smooth: {
                    type: 'continuous'
                }
            }
        };
        var network = new vis.Network(container, data, options);
    });
}
// create an array with nodes
