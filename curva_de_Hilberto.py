import matplotlib.pyplot as plt
import sys

def hilbert(x, y, xi, xj, yi, yj, n, pts):
    if n <= 0:
        pts.append((x + (xi + yi) / 2, y + (xj + yj) / 2))
        return
    
    # Subdivisión recursiva rotando y trasladando los sub-cuadrados
    hilbert(x, y, yi/2, yj/2, xi/2, xj/2, n-1, pts)
    hilbert(x + xi/2, y + xj/2, xi/2, xj/2, yi/2, yj/2, n-1, pts)
    hilbert(x + xi/2 + yi/2, y + xj/2 + yj/2, xi/2, xj/2, yi/2, yj/2, n-1, pts)
    hilbert(x + xi/2 + yi, y + xj/2 + yj, -yi/2, -yj/2, -xi/2, -xj/2, n-1, pts)

def generar_curva_hilbert(orden):
    puntos = []
    # Parámetros iniciales del plano unitario (0,0) a (1,1)
    hilbert(0, 0, 1, 0, 0, 1, orden, puntos)
    return puntos

orden = int(sys.argv[1])
res=generar_curva_hilbert(orden)
plt.figure(figsize=(8, 8))
x, y = zip(*res)
plt.plot(x, y, marker='o', markersize=4, linestyle=':', color='red')
plt.title(f"Curva de Hilberto de orden {orden}")
plt.axis('equal')
plt.show()
