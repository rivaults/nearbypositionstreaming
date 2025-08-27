import Color from 'https://unpkg.com/color@5.0.2/index.js?module';

window.dash_utils = Object.assign({}, window.dash_utils, {
    get_pastel_rgb_color: function() {
        const r = Math.floor(Math.random() * 256);
        const g = Math.floor(Math.random() * 256);
        const b = Math.floor(Math.random() * 256);
        const res = Color.rgb(r, g, b).saturate(0.1).mix(Color("white"));
        return res.hex().toString();
    },
});
