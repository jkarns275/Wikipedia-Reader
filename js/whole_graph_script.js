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

    httpRequest("/network", "POST", function(response) {
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
            nodes: {
                shape: 'dot',
                scaling: {
                    min: 10,
                    max: 80
                },
                font: {
                    size: 10,
                    face: 'Tahoma'
                }
            },
            edges: {
                width: 0.15,
                color: {inherit: 'from'},
                smooth: {
                    type: 'continuous'
                }
            },
            physics: {
                stabilization: false,
                barnesHut: {
                    gravitationalConstant: -80000,
                    springConstant: 0.001,
                    springLength: 200
                }
            },
            interaction: {
                tooltipDelay: 200,
                hideEdgesOnDrag: true
            }
        };
        var network = new vis.Network(container, data, options);
        network.clusterOutliers();
    });
}
// create an array with nodes
