import random
import time

class Persona:
    def __init__(self, nombre, punto_partida, punto_destino):
        self.nombre = nombre
        self.punto_partida = punto_partida
        self.punto_destino = punto_destino

class Coche:
    def __init__(self, capacidad):
        self.capacidad = capacidad
        self.viajeros = []
        self.ubicacion_actual = None

    def agregar_viajero(self, viajero):
        if len(self.viajeros) < self.capacidad:
            self.viajeros.append(viajero)
            print(f"{viajero.nombre} ha abordado el coche.")
        else:
            print("El coche está lleno. No se puede agregar más viajeros.")

    def viajar_hacia_destino(self):
        print("\nIniciando viaje hacia el destino...")
        for i in range(1, 6):  # Simulamos 5 pasos del viaje
            self.ubicacion_actual = (
                round(random.uniform(self.ubicacion_actual[0] - 0.5, self.ubicacion_actual[0] + 0.5), 2),
                round(random.uniform(self.ubicacion_actual[1] - 0.5, self.ubicacion_actual[1] + 0.5), 2)
            )
            print(f"Paso {i}: Coche en {self.ubicacion_actual}")
            time.sleep(1)  # Simulamos un segundo de tiempo de viaje

        print("\n¡Destino alcanzado!")

def generar_coordenadas():
    return round(random.uniform(0, 10), 2), round(random.uniform(0, 10), 2)

def main():
    # Generar coordenadas para los puntos A y B
    punto_a = generar_coordenadas()
    punto_b = generar_coordenadas()

    # Crear la persona que solicita el coche
    persona_solicitante = Persona("Persona", punto_a, punto_b)

    # Generar coordenadas para un nuevo viajero cercano al punto de partida
    punto_nuevo_viajero = (
        round(random.uniform(persona_solicitante.punto_partida[0] - 1, persona_solicitante.punto_partida[0] + 1), 2),
        round(random.uniform(persona_solicitante.punto_partida[1] - 1, persona_solicitante.punto_partida[1] + 1), 2)
    )

    nuevo_viajero = Persona("Nuevo Viajero", punto_nuevo_viajero, persona_solicitante.punto_destino)

    # Crear el coche
    coche = Coche(capacidad=2)
    coche.ubicacion_actual = punto_a

    # Agregar la persona solicitante y el nuevo viajero al coche
    coche.agregar_viajero(persona_solicitante)
    coche.agregar_viajero(nuevo_viajero)

    # Imprimir información sobre el viaje
    print(f"\nPunto de partida: {persona_solicitante.punto_partida}")
    print(f"Punto de destino: {persona_solicitante.punto_destino}")
    print(f"Ubicación del nuevo viajero: {nuevo_viajero.punto_partida}")
    print("\nComenzando el viaje...")

    # Iniciar el viaje hacia el destino
    coche.viajar_hacia_destino()

if __name__ == "__main__":
    main()
