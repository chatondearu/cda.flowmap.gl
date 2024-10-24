export default `\
#version 300 es
#define SHADER_NAME animated-flow-lines-layer-fragment-shader

precision highp float;

uniform float animationTailLength;

in vec4 vColor;
in float sourceToTarget;
in vec2 uv;

out vec4 fragColor;

void main(void) {
  vec2 geometryUV = uv;
  float alpha = smoothstep(1.0 - animationTailLength, 1.0, fract(sourceToTarget));
  fragColor = vec4(vColor.rgb, vColor.a * alpha);

  // Assuming DECKGL_FILTER_COLOR is a macro or function defined elsewhere
  DECKGL_FILTER_COLOR(fragColor, geometryUV);
}
`;
