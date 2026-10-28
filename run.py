from collections import defaultdict
from pathlib import Path
from tempfile import TemporaryDirectory
import argparse
from flask import Flask, render_template_string, request
from werkzeug.utils import secure_filename
import reglas
import topologias as topology_runner

app = Flask(__name__)

PAGE = """<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Validador topológico</title>
  <style>
	:root{font-family:Inter,system-ui,-apple-system,"Segoe UI",sans-serif;color:#17231f;background:#f6f9f7}
	*{box-sizing:border-box}body{margin:0}.wrap{max-width:1000px;margin:auto;padding:54px 24px}
	.eyebrow{color:#147d58;text-transform:uppercase;letter-spacing:.14em;font-size:12px;font-weight:700}
	h1{font-size:clamp(32px,5vw,44px);letter-spacing:-.05em;margin:8px 0}.lead{color:#718078;max-width:650px;margin:0 0 30px}
	.grid{display:grid;grid-template-columns:1fr 1fr;gap:20px;align-items:start}.card{background:white;border:1px solid #e3ebe5;border-radius:16px;padding:24px;box-shadow:0 8px 28px #193b260d}
	h2{font-size:18px;margin:0 0 5px}.hint,.help,.sub{color:#718078;font-size:13px}.hint{margin:0 0 20px}
	label{display:block;font-weight:650;font-size:13px;margin:17px 0 7px}textarea{width:100%;min-height:120px;resize:vertical;border:1px solid #dce5df;border-radius:10px;padding:12px;font:13px/1.6 ui-monospace,monospace;outline:none}
	textarea:focus{border-color:#64aa89;box-shadow:0 0 0 3px #147d5818}.help{font-size:12px;margin:6px 0 0}
	button{width:100%;margin-top:20px;padding:12px;border:0;border-radius:10px;background:#147d58;color:white;font:inherit;font-weight:650;cursor:pointer}button:hover{background:#106747}
	.headline{display:flex;align-items:center;gap:12px;margin:2px 0 20px}.dot{width:11px;height:11px;border-radius:50%;background:#dfa52d;box-shadow:0 0 0 5px #dfa52d1c}.dot.ok{background:#1b9b68;box-shadow:0 0 0 5px #1b9b6818}.dot.bad{background:#d95757;box-shadow:0 0 0 5px #d9575718}
	.title{font-weight:700}.sub{font-size:12px}.stats{display:grid;grid-template-columns:repeat(3,1fr);gap:9px;margin:20px 0}.stat{padding:12px;border-radius:10px;background:#f3f8f4}.stat strong{display:block;font-size:20px}.stat span{font-size:11px;color:#718078}
	ul{list-style:none;padding:0;margin:13px 0 0}li{display:flex;align-items:flex-start;gap:10px;padding:11px 0;border-top:1px solid #edf1ee;font-size:13px}.mark{flex:none;width:20px;height:20px;display:grid;place-items:center;border-radius:50%;background:#e9f5ee;color:#147d58;font-weight:800}.mark.warn{background:#fff4d9;color:#986a11}.mark.error{background:#fdeaea;color:#b63838}.empty{color:#718078;font-size:13px;margin-top:18px}
	@media(max-width:700px){.wrap{padding:34px 16px}.grid{grid-template-columns:1fr}}
  </style>
</head>
<body><main class="wrap">
  <div class="eyebrow">Herramienta de análisis</div><h1>Validador topológico</h1>
  <p class="lead">Comprueba la estructura y consistencia de tu topología de red. Introduce los nodos y sus conexiones para obtener un resumen de validación.</p>
  <div class="grid">
	<section class="card"><h2>Datos de la topología</h2><p class="hint">Define los elementos y sus conexiones no dirigidas.</p>
	  <form method="post" enctype="multipart/form-data">
		<label for="folder">Carpeta de datos</label><input id="folder" name="folder" type="file" webkitdirectory directory multiple required>
		<p class="help">Selecciona la carpeta que contiene los datos de la topología.</p>
		<label for="topologia">Topología</label><select id="topologia" name="topologia" required>
		  {% for value, label in topology_options %}<option value="{{ value }}">{{ label }}</option>{% endfor %}
		</select>
		<button type="submit">Validar</button>
	  </form>
	</section>
	<section class="card" aria-live="polite">
	  {% if result is not none %}
		<h2>Resultado</h2><p class="empty">{{ result }}</p>
	  {% else %}
		<h2>Resultado</h2><p class="empty">Aquí aparecerá el resultado de la validación.</p>
	  {% endif %}
	</section>
  </div>
</main></body></html>"""


def validate_topology(raw_nodes, raw_edges):
	checks = []

	def report(level, message):
		checks.append((level, message))

	entries = [item.strip() for item in raw_nodes.replace("\n", ",").split(",") if item.strip()]
	nodes = set(entries)
	if len(entries) != len(nodes):
		report("error", f"Hay {len(entries) - len(nodes)} nodo(s) duplicado(s).")
	else:
		report("ok", "No hay nodos duplicados.")

	adjacency = defaultdict(set)
	edges = set()
	invalid = repeated = loops = 0
	for line in raw_edges.splitlines():
		if not line.strip():
			continue
		pair = [item.strip() for item in line.split(",")]
		if len(pair) != 2 or not all(pair) or pair[0] not in nodes or pair[1] not in nodes:
			invalid += 1
			continue
		left, right = pair
		if left == right:
			loops += 1
			continue
		edge = tuple(sorted((left, right)))
		if edge in edges:
			repeated += 1
			continue
		edges.add(edge)
		adjacency[left].add(right)
		adjacency[right].add(left)

	report("error" if invalid else "ok", f"{invalid} conexión(es) inválidas o con nodos no declarados." if invalid else "Todas las conexiones tienen formato válido.")
	report("error" if loops else "ok", f"Se detectaron {loops} bucle(s) propios." if loops else "No hay conexiones de un nodo consigo mismo.")
	report("warn" if repeated else "ok", f"Se ignoraron {repeated} conexión(es) repetidas." if repeated else "No hay conexiones repetidas.")

	components = 0
	visited = set()
	for node in nodes:
		if node in visited:
			continue
		components += 1
		stack = [node]
		visited.add(node)
		while stack:
			current = stack.pop()
			for neighbor in adjacency[current] - visited:
				visited.add(neighbor)
				stack.append(neighbor)
	if not nodes:
		report("error", "Añade al menos un nodo para validar la topología.")
	elif components == 1:
		report("ok", "La topología está conectada.")
	else:
		report("warn", f"La topología está desconectada y contiene {components} componentes.")

	parent = {node: node for node in nodes}

	def find(node):
		while parent[node] != node:
			parent[node] = parent[parent[node]]
			node = parent[node]
		return node

	cycle = False
	for left, right in edges:
		root_left, root_right = find(left), find(right)
		if root_left == root_right:
			cycle = True
			break
		parent[root_left] = root_right
	report("ok", "Se detectaron ciclos en la topología." if cycle else "No se detectaron ciclos; la estructura es un bosque.")

	errors = any(level == "error" for level, _ in checks)
	warnings = any(level == "warn" for level, _ in checks)
	status = "bad" if errors else ("warn" if warnings else "ok")
	title = "Revisa los datos" if errors else ("Validación con observaciones" if warnings else "Topología válida")
	summary = "Se encontraron errores que requieren atención." if errors else ("La estructura es válida, con observaciones." if warnings else "Todas las comprobaciones finalizaron correctamente.")
	return {"checks": checks, "node_count": len(nodes), "edge_count": len(edges), "components": components, "status": status, "title": title, "summary": summary}


@app.route("/", methods=["GET", "POST"])
def index():
	topologies = reglas.topologicas
	entries = list(topologies.items()) if isinstance(topologies, dict) else list(enumerate(topologies))
	topology_options = [(str(index), str(label)) for index, (label, _) in enumerate(entries)]
	result = None
	if request.method == "POST":
		files = request.files.getlist("folder")
		selected_index = request.form.get("topologia", "")
		if not files or not any(file.filename for file in files):
			result = "Selecciona una carpeta que contenga archivos."
		elif not selected_index.isdigit() or int(selected_index) >= len(entries):
			result = "Selecciona una topología válida."
		else:
			selected_topology = entries[int(selected_index)][1]
			try:
				# with TemporaryDirectory() as folder_path:
				# 	for uploaded_file in files:
				# 		if not uploaded_file.filename:
				# 			continue
				# 		parts = [secure_filename(part) for part in Path(uploaded_file.filename.replace("\\", "/")).parts if part not in ("", ".", "..", "/")]
				# 		if parts:
				# 			destination = Path(folder_path).joinpath(*parts)
				# 			destination.parent.mkdir(parents=True, exist_ok=True)
				# 			uploaded_file.save(destination)
				print(selected_topology,files)
				result = topology_runner.inicio(argparse.Namespace(capa=selected_topology,folder_datos=files))
			except Exception as error:
				result = f"No se pudo ejecutar la validación: {error}"
	return render_template_string(PAGE, topology_options=topology_options, result=result)


if __name__ == "__main__":
	app.run(debug=True)
