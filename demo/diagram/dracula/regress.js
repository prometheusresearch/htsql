var redraw;

window.onload = function() {
    var width = $(document).width();
    var height = $(document).height() - 100;

    /* Showcase of the Bellman-Ford search algorithm finding shortest paths 
       from one point to every node */
    
    /*  */

    /* We need to write a new node renderer function to display the computed
       distance.
       (the Raphael graph drawing implementation of Dracula can draw this shape,
       please consult the RaphaelJS reference for details http://raphaeljs.com/) */
    var render = function(r, n) {
            /* the Raphael set is obligatory, containing all you want to display */
            var set = r.set().push(
                /* custom objects go here */
                r.rect(n.point[0]-30, n.point[1]-13, 60, 44).attr({"fill": "#feb", r : "12px", "stroke-width" : n.distance == 0 ? "3px" : "1px" })).push(
                r.text(n.point[0], n.point[1] + 10, (n.label || n.id) + "\n(" + (n.distance == undefined ? "Infinity" : n.distance) + ")"));
            return set;
        };
    
    var g = new Graph();
    
    
    /* creating nodes and passing the new renderer function to overwrite the default one */
    g.addNode("school", {render:render});
    g.addNode("course_classification", {render:render});
    g.addNode("appointment", {render:render});
    g.addNode("classification", {render:render});
    g.addNode("confidential", {render:render});
    g.addNode("program_requirement", {render:render});
    g.addNode("enrollment", {render:render});
    g.addNode("course", {render:render});
    g.addNode("semester", {render:render});
    g.addNode("program", {render:render});
    g.addNode("student", {render:render});
    g.addNode("department", {render:render});
    g.addNode("instructor", {render:render});
    g.addNode("class", {render:render});
    g.addNode("prerequisite", {render:render});

    /* connections */
    g.addEdge("school", "department", {}, 1)
    g.addEdge("school", "program", {}, 1)
    g.addEdge("course_classification", "course", {}, 2)
    g.addEdge("course_classification", "classification", {}, 2)
    g.addEdge("appointment", "department", {}, 2)
    g.addEdge("appointment", "instructor", {}, 2)
    g.addEdge("classification", "classification", {}, 1)
    g.addEdge("classification", "classification", {}, 1)
    g.addEdge("classification", "program_requirement", {}, 1)
    g.addEdge("classification", "course_classification", {}, 1)
    g.addEdge("confidential", "instructor", {}, 2)
    g.addEdge("program_requirement", "classification", {}, 2)
    g.addEdge("program_requirement", "program", {}, 2)
    g.addEdge("enrollment", "class", {}, 2)
    g.addEdge("enrollment", "student", {}, 2)
    g.addEdge("course", "department", {}, 2)
    g.addEdge("course", "prerequisite", {}, 1)
    g.addEdge("course", "course_classification", {}, 1)
    g.addEdge("course", "class", {}, 1)
    g.addEdge("semester", "class", {}, 1)
    g.addEdge("program", "school", {}, 2)
    g.addEdge("program", "program", {}, 1)
    g.addEdge("program", "student", {}, 1)
    g.addEdge("program", "program_requirement", {}, 1)
    g.addEdge("program", "program", {}, 1)
    g.addEdge("student", "program", {}, 1)
    g.addEdge("student", "enrollment", {}, 1)
    g.addEdge("department", "school", {}, 1)
    g.addEdge("department", "appointment", {}, 1)
    g.addEdge("department", "course", {}, 1)
    g.addEdge("instructor", "confidential", {}, 1)
    g.addEdge("instructor", "appointment", {}, 1)
    g.addEdge("instructor", "class", {}, 1)
    g.addEdge("class", "course", {}, 2)
    g.addEdge("class", "semester", {}, 2)
    g.addEdge("class", "instructor", {}, 1)
    g.addEdge("class", "enrollment", {}, 1)
    g.addEdge("prerequisite", "course", {}, 2)

    /* layout the graph using the Spring layout implementation */
    var layouter = new Graph.Layout.Spring(g);
    
    /* draw the graph using the RaphaelJS draw implementation */

    /* calculating the shortest paths via Bellman Ford */
//    bellman_ford(g, g.nodes["Berlin"]);
    
    /* calculating the shortest paths via Dijkstra */
    dijkstra(g, g.nodes["school"]);
    
    /* calculating the shortest paths via Floyd-Warshall */
    floyd_warshall(g, g.nodes["school"]);


    /* colourising the shortest paths and setting labels */
    for(e in g.edges) {
        if(g.edges[e].target.predecessor === g.edges[e].source || g.edges[e].source.predecessor === g.edges[e].target) {
            g.edges[e].style.stroke = "#bfa";
            g.edges[e].style.fill = "#56f";
        } else {
            g.edges[e].style.stroke = "#aaa";
        }
    }
    
    var renderer = new Graph.Renderer.Raphael('canvas', g, width, height);

    redraw = function() {
        layouter.layout();
        renderer.draw();
    };
    
/*    var pos=0;
    step = function(dir) {
        pos+=dir;
        var renderer = new Graph.Renderer.Raphael('canvas', g.snapshots[pos], width, height);
        renderer.draw();
    };*/
};
