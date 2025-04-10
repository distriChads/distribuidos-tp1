package main

import (
	"fmt"
	"strconv"
	"strings"
)

func FilterBySpainAndOf2000(lines []string) []string {
	var result []string
	for _, line := range lines {
		parts := strings.Split(line, ",")
		raw_year := strings.Split(parts[1], "-")[0]
		year, err := strconv.Atoi(raw_year)
		if err != nil {
			continue
		}
		if !(year >= 2000 && year < 2010) {
			continue
		}
		countries := strings.Split(parts[0], "|")
		for _, country := range countries {
			if strings.TrimSpace(country) == "SPAIN" {
				result = append(result, strings.TrimSpace(line))
				break
			}
		}

	}
	return result
}

func main() {
	// Cambiar a recibir por rabbitmq el mensajito
	// se asume que el segundo valor va a ser el año
	line := `
	USA|ARG|CHI,2002-10-30,Comedy|Family|Action,toy story,id1
	CAN|ARG|BRA,1996-10-30,Comedy|Family|Action,megamente,id2
	ARG|CHI|SPAIN,2009-10-30,Comedy|Family|Action,shrek,id3
	USA,1992-10-30,Comedy|Family|Action,cars,id4
	`

	lineas := strings.Split(strings.TrimSpace(line), "\n")

	resultado := FilterBySpainAndOf2000(lineas)

	for _, r := range resultado {

		//ESTE SPLIT Y OBTENER TITLE Y GENRE LO HARIA EL PROTOCOLO
		parts := strings.Split(r, ",")
		title_and_id := strings.TrimSpace(parts[len(parts)-3]) + "," + strings.TrimSpace(parts[len(parts)-2])

		// Cambiar a simplemente enviar los datos :)
		fmt.Println(title_and_id)
	}
}
