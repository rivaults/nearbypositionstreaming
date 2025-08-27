const TYPE_IN = 0;
const TYPE_NEARBY= 1;

window.dash_clientside = Object.assign({}, window.dash_clientside, {
    clientside: {

        ws_send: function(id, state) {
            if (state['state_id'] != id){
                this.clear(id);
            }
            return id;
        },

        clear: function(id) {
            dash_clientside.set_props("phone", {positions: []});
            dash_clientside.set_props("nearby", {children: [], orders: {}});
            dash_clientside.set_props("store-state", {data: id});
        },

        create_pattern: function(){
            const rnd_color = window.dash_utils.get_pastel_rgb_color();
            return [
                    {offset:"0", repeat:"1", dash:{pixelSize:6, pathOptions:{color:rnd_color, weight:6}}},
                    {offset:"0", repeat:"25", arrowHead:{pixelSize:3, pathOptions:{color:"#fff", stroke:true}}}
            ];
        },

        merge_positions: function(positions, data){
            if (positions.length == 0)
                positions = [[]];
            const merged = positions.at(-1).concat(data["points"][0]);
            return [merged, ...data["points"].slice(1)];
        },

        receive_phone: function(data, old_positions){
            const positions = this.merge_positions(old_positions, data);
            dash_clientside.set_props("phone", {positions: positions});
            dash_clientside.set_props("map", {center: positions.at(-1).at(-1)});
        },

        get_id: function(nearby_id){
            return `nearby-${nearby_id}`;
        },

        create_nearby: function(nearby_id, new_positions){
            return React.createElement(window.dash_leaflet.PolylineDecorator, {
                id: nearby_id,
                patterns: this.create_pattern(),
                positions: new_positions,
                setProps: (props) => {
                    return props
                }
            }, children=[]);
        },

        receive_nearby: function(data, children, orders){
            if (orders === undefined)
                orders = {}
            const nearby_id = this.get_id(data['id']);
            let old_positions = [];
            let child;
            if (orders.hasOwnProperty(nearby_id)){
                child = children[orders[nearby_id]];
                old_positions = child.props.positions;
            }
            const positions = this.merge_positions(old_positions, data);
            if (orders.hasOwnProperty(nearby_id)){
                dash_clientside.set_props(nearby_id, {positions: positions});
                return window.dash_clientside.no_update;
            }
            orders[nearby_id] = children.length;
            dash_clientside.set_props("nearby", {orders: orders});
            return [...children, this.create_nearby(nearby_id, positions)];
        },

        ws_receive: function(message, positions, children, orders) {
            const _data = JSON.parse(message["data"]);
            if (_data["type"] == TYPE_IN)
                this.receive_phone(_data, positions);
            else if (_data["type"] == TYPE_NEARBY)
                return this.receive_nearby(_data, children, orders);
            return window.dash_clientside.no_update;
        }
    }
});