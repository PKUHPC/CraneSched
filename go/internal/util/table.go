/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package util

import (
	"strings"

	"github.com/olekukonko/tablewriter"
)

func SetBorderlessTable(table *tablewriter.Table) {
	table.SetBorder(false)
	table.SetAutoFormatHeaders(true)
	table.SetAutoWrapText(false)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetHeaderLine(false)
	table.SetCenterSeparator("")
	table.SetColumnSeparator("")
	table.SetRowSeparator("")
	table.SetTablePadding(" ")
	table.SetNoWhiteSpace(true)
}

func SetBorderTable(table *tablewriter.Table) {
	table.SetBorders(tablewriter.Border{Left: true, Top: true, Right: true, Bottom: true})
	table.SetAutoFormatHeaders(true)
	table.SetAutoWrapText(true)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetCenterSeparator("|")
	table.SetTablePadding("\t")
}

func FormatTable(tableOutputWidth []int, tableHeader []string,
	tableData [][]string, rightAlign ...[]bool) (formatTableHeader []string, formatTableData [][]string) {
	// If rightAlign is not provided, default to all left-aligned
	var alignments []bool
	if len(rightAlign) > 0 && rightAlign[0] != nil {
		alignments = rightAlign[0]
	} else {
		alignments = make([]bool, len(tableHeader))
	}

	for i, h := range tableHeader {
		if tableOutputWidth[i] != -1 {
			padLength := tableOutputWidth[i] - len(h)
			if padLength >= 0 {
				if i < len(alignments) && alignments[i] {
					// Right align: pad on the left
					tableHeader[i] = strings.Repeat(" ", padLength) + h
				} else {
					// Left align: pad on the right
					tableHeader[i] = h + strings.Repeat(" ", padLength)
				}
			} else {
				tableHeader[i] = h[:tableOutputWidth[i]]
			}
		}
	}
	for i, row := range tableData {
		for j, cell := range row {
			if tableOutputWidth[j] != -1 {
				padLength := tableOutputWidth[j] - len(cell)
				if padLength >= 0 {
					if j < len(alignments) && alignments[j] {
						// Right align: pad on the left
						tableData[i][j] = strings.Repeat(" ", padLength) + cell
					} else {
						// Left align: pad on the right
						tableData[i][j] = cell + strings.Repeat(" ", padLength)
					}
				} else {
					tableData[i][j] = cell[:tableOutputWidth[j]]
				}
			}
		}
	}
	return tableHeader, tableData
}

// Trim the cell of the table, if the cell is longer than 30 characters,
// the rest of the characters will be replaced by `...`.
// `excepts` means do not trim the specified columns
func TrimTableExcept(rows *[][]string, excepts ...int) {
	removal := make(map[int]bool)
	for _, except := range excepts {
		removal[except] = true
	}

	for i, row := range *rows {
		for j, cell := range row {
			if removal[j] {
				continue
			}
			if len(cell) > 30 {
				(*rows)[i][j] = cell[:30] + "..."
			}
		}
	}
}

// Trim the cell of the table, if the cell is longer than 30 characters,
// the rest of the characters will be replaced by `...`.
func TrimTable(rows *[][]string) {
	TrimTableExcept(rows)
}

// split String by the seps that you provide for
func SplitString(str string, seps []string) []string {
	if len(seps) == 0 {
		return []string{str}
	}

	result := strings.Split(str, seps[0])

	if len(seps) > 1 {
		var finalResult []string
		for _, part := range result {
			finalResult = append(finalResult, SplitString(part, seps[1:])...)
		}
		result = finalResult
	}
	// clean the space before string or after string
	var cleanedResult []string
	for _, part := range result {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			cleanedResult = append(cleanedResult, trimmed)
		}
	}
	return cleanedResult
}
