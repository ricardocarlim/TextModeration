import onnx
m = onnx.load("/m/model.onnx")
print("=== INPUTS ===")
for i in m.graph.input:
    dims = [d.dim_value if d.dim_value else d.dim_param for d in i.type.tensor_type.shape.dim]
    print(i.name, dims)
print("=== OUTPUTS ===")
for o in m.graph.output:
    dims = [d.dim_value if d.dim_value else d.dim_param for d in o.type.tensor_type.shape.dim]
    print(o.name, dims)
