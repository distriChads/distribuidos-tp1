package main

import (
	"fmt"
	"strings"
)

func FilterByCountryArgentina(lines []string) []string {
	var result []string
	for _, line := range lines {
		parts := strings.Split(line, ",")
		countries := strings.Split(parts[0], "|")
		for _, country := range countries {
			if strings.TrimSpace(country) == "ARG" {
				result = append(result, strings.TrimSpace(line))
				break
			}
		}
	}
	return result
}

func main() {
	// Cambiar a recibir por rabbitmq el mensajito
	line := `
	USA|ARG|CHI,2002-10-30,toy story,tuki
	CAN|ARG|BRA,1996-10-30,toy story,tuki
	ARG|CHI,2004-10-30,toy story,tuki
	USA,2011-10-30,toy story,viva
	`

	lineas := strings.Split(strings.TrimSpace(line), "\n")

	resultado := FilterByCountryArgentina(lineas)
	for _, r := range resultado {
		// cambiar por simplemente enviar el paquete a ambas colas, este filtro recibe, filtra y envia como le llego
		fmt.Println(r)
	}
}
