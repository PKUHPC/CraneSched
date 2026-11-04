/**
 * Copyright (c) 2025 Peking University and Peking University
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

package cacctmgr

import (
	"CraneFrontEnd/internal/util"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

type CAcctMgrCommand struct {
	Command any `parser:"@@"`
}

type AddCommand struct {
	Action      string           `parser:"@'add'"`
	Entity      *EntityType      `parser:"@@"`
	ID          string           `parser:"( (?! Ident AssignOp) ( @String | @Ident | @Number ) )?"`
	KVParams    []*KeyValueParam `parser:"@@*"`
	GlobalFlags []*Flag          `parser:"@@*"`
}

type DeleteCommand struct {
	Action      string           `parser:"@'delete'"`
	Entity      *EntityType      `parser:"@@"`
	ID          string           `parser:"( (?! Ident AssignOp) ( @String | @Ident | @Number ) )?"`
	KVParams    []*KeyValueParam `parser:"@@*"`
	GlobalFlags []*Flag          `parser:"@@*"`
}

type BlockCommand struct {
	Action      string           `parser:"@'block'"`
	Entity      *EntityType      `parser:"@@"`
	ID          string           `parser:"( (?! Ident AssignOp) ( @String | @Ident | @Number ) )?"`
	KVParams    []*KeyValueParam `parser:"@@*"`
	GlobalFlags []*Flag          `parser:"@@*"`
}

type UnblockCommand struct {
	Action      string           `parser:"@'unblock'"`
	Entity      *EntityType      `parser:"@@"`
	ID          string           `parser:"( (?! Ident AssignOp) ( @String | @Ident | @Number ) )?"`
	KVParams    []*KeyValueParam `parser:"@@*"`
	GlobalFlags []*Flag          `parser:"@@*"`
}

type ModifyCommand struct {
	Action      string           `parser:"@('modify' | 'update')"`
	Entity      *EntityType      `parser:"@@"`
	KVParams    []*KeyValueParam `parser:"@@*"`
	Where       *WhereClause     `parser:"@@?"`
	Set         *SetClause       `parser:"@@?"`
	GlobalFlags []*Flag          `parser:"@@*"`
}

type ShowCommand struct {
	Action      string           `parser:"@'show'"`
	Entity      *EntityType      `parser:"@@?"`
	ID          string           `parser:"( (?! Ident AssignOp) ( @String | @Ident | @Number ) )?"`
	Where       *WhereClause     `parser:"@@?"`
	KVParams    []*KeyValueParam `parser:"@@*"`
	GlobalFlags []*Flag          `parser:"@@*"`
}

type ResetCommand struct {
	Action      string           `parser:"@'reset'"`
	ID          string           `parser:"( (?! Ident AssignOp) ( @String | @Ident | @Number ) )?"`
	KVParams    []*KeyValueParam `parser:"@@*"`
	GlobalFlags []*Flag          `parser:"@@*"`
}

type EntityType struct {
	Account  bool `parser:"@'account'"`
	User     bool `parser:"| @'user'"`
	Qos      bool `parser:"| @'qos'"`
	Txn      bool `parser:"| @'transaction'"`
	Event    bool `parser:"| @'event'"`
	Wckey    bool `parser:"| @'wckey'"`
	Resource bool `parser:"| @'resource'"`
}

type Flag struct {
	Name  string `parser:"'-' '-'? @Ident"`
	Value string `parser:"( '=' (@String | @Time | @Ident | @Number) | (@String | @Time | @Ident | @Number) )?"`
}

type KeyValueParam struct {
	Key   string `parser:"@Ident"`
	Value string `parser:"( '=' (@String | @Time | @Ident | @Number | @Duration) | (@String | @Time | @Ident | @Number | @Duration) )?"`
}

type WhereParam struct {
	Key   string `parser:"@Ident"`
	Value string `parser:"( '=' ( @String | @Time | @Ident | @Number) | @String | @Time | @Ident | @Number )"`
}

type WhereClause struct {
	Where       string        `parser:"@WHERE"`
	WhereParams []*WhereParam `parser:"@@*"`
}

type SetParam struct {
	Key   string `parser:"@Ident"`
	Op    string `parser:"@AssignOp?"`
	Value string `parser:"@(String | Ident | Number | Duration)"`
}

type SetClause struct {
	Set       string      `parser:"@SET"`
	SetParams []*SetParam `parser:"@@*"`
}

var CAcctMgrLexer = lexer.MustSimple([]lexer.SimpleRule{
	{Name: "whitespace", Pattern: `\s+`},
	{Name: "SET", Pattern: `set`},
	{Name: "WHERE", Pattern: `where`},
	{Name: "AssignOp", Pattern: `(\+=|\-=|=)`},
	{Name: "String", Pattern: `("[^"]*"|'[^']*'|""|'')`},
	{Name: "Time", Pattern: `(?:~)?[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:~)?`},
	{Name: "Duration", Pattern: `\d+(?:-\d+(?::\d+){0,2}|(?::\d+){1,2})`},
	{Name: "Number", Pattern: `[-+]?\d+(\.\d+)?`},
	{Name: "Ident", Pattern: `[a-zA-Z0-9][a-zA-Z0-9_\@\.,:\[\]T/]*`},
	{Name: "Punct", Pattern: `[-,:]`},
})

var CAcctMgrParser = participle.MustBuild[CAcctMgrCommand](
	participle.Lexer(CAcctMgrLexer),
	participle.Elide("whitespace"),
	participle.Union[any](AddCommand{}, DeleteCommand{}, BlockCommand{}, UnblockCommand{}, ModifyCommand{}, ShowCommand{}, ResetCommand{}),
)

func ParseCAcctMgrCommand(input string) (*CAcctMgrCommand, error) {
	return CAcctMgrParser.ParseString("", input)
}

func (r EntityType) String() string {
	switch {
	case r.Account:
		return "account"
	case r.User:
		return "user"
	case r.Qos:
		return "qos"
	case r.Txn:
		return "transaction"
	case r.Event:
		return "event"
	case r.Wckey:
		return "wckey"
	case r.Resource:
		return "resource"
	default:
		return ""
	}
}

func (c *CAcctMgrCommand) GetAction() string {
	switch cmd := c.Command.(type) {
	case AddCommand:
		return cmd.Action
	case DeleteCommand:
		return cmd.Action
	case BlockCommand:
		return cmd.Action
	case UnblockCommand:
		return cmd.Action
	case ModifyCommand:
		return cmd.Action
	case ShowCommand:
		return cmd.Action
	case ResetCommand:
		return cmd.Action
	default:
		return ""
	}
}

func (c *CAcctMgrCommand) GetEntity() string {
	switch cmd := c.Command.(type) {
	case AddCommand:
		if cmd.Entity != nil {
			return cmd.Entity.String()
		}
	case DeleteCommand:
		if cmd.Entity != nil {
			return cmd.Entity.String()
		}
	case BlockCommand:
		if cmd.Entity != nil {
			return cmd.Entity.String()
		}
	case UnblockCommand:
		if cmd.Entity != nil {
			return cmd.Entity.String()
		}
	case ModifyCommand:
		if cmd.Entity != nil {
			return cmd.Entity.String()
		}
	case ShowCommand:
		if cmd.Entity != nil {
			return cmd.Entity.String()
		}
	}
	return ""
}

func (c *CAcctMgrCommand) GetKVParamValue(key string) string {
	var params []*KeyValueParam

	switch cmd := c.Command.(type) {
	case AddCommand:
		params = cmd.KVParams
	case DeleteCommand:
		params = cmd.KVParams
	case BlockCommand:
		params = cmd.KVParams
	case UnblockCommand:
		params = cmd.KVParams
	case ShowCommand:
		params = cmd.KVParams
	case ResetCommand:
		params = cmd.KVParams
	default:
		return ""
	}

	for _, param := range params {
		if strings.EqualFold(param.Key, key) {
			return unquoteIfQuoted(param.Value)
		}
	}
	return ""
}

// GetFilterValue returns a show filter given either as Key=value or inside a
// where clause, so both the documented and the Slurm-style forms work.
func (c *CAcctMgrCommand) GetFilterValue(key string) string {
	if value := c.GetKVParamValue(key); value != "" {
		return value
	}
	for whereKey, value := range c.GetWhereParams() {
		if strings.EqualFold(whereKey, key) {
			return value
		}
	}
	return ""
}

func (c *CAcctMgrCommand) GetID() string {
	switch cmd := c.Command.(type) {
	case AddCommand:
		return cmd.ID
	case DeleteCommand:
		return cmd.ID
	case BlockCommand:
		return cmd.ID
	case UnblockCommand:
		return cmd.ID
	case ShowCommand:
		return cmd.ID
	case ResetCommand:
		return cmd.ID
	default:
		return ""
	}
}

func (c *CAcctMgrCommand) GetKVMaps() map[string]string {
	kvMap := make(map[string]string)
	var params []*KeyValueParam

	switch cmd := c.Command.(type) {
	case AddCommand:
		params = cmd.KVParams
	case DeleteCommand:
		params = cmd.KVParams
	case BlockCommand:
		params = cmd.KVParams
	case UnblockCommand:
		params = cmd.KVParams
	case ShowCommand:
		params = cmd.KVParams
	case ResetCommand:
		params = cmd.KVParams
	case ModifyCommand:
		params = cmd.KVParams
	default:
		return kvMap
	}

	for _, param := range params {
		kvMap[param.Key] = unquoteIfQuoted(param.Value)
	}
	return kvMap
}

func (c *CAcctMgrCommand) GetWhereParams() map[string]string {
	whereMap := make(map[string]string)

	switch cmd := c.Command.(type) {
	case ModifyCommand:
		if cmd.Where != nil {
			for _, param := range cmd.Where.WhereParams {
				whereMap[param.Key] = unquoteIfQuoted(param.Value)
			}
		}
	case ShowCommand:
		if cmd.Where != nil {
			for _, param := range cmd.Where.WhereParams {
				whereMap[param.Key] = unquoteIfQuoted(param.Value)
			}
		}
	default:
		return nil
	}
	return whereMap
}

func (c *CAcctMgrCommand) GetSetParams() (map[string]string, map[string]string, map[string]string) {
	setMap := make(map[string]string)
	addMap := make(map[string]string)
	deleteMap := make(map[string]string)

	switch cmd := c.Command.(type) {
	case ModifyCommand:
		if cmd.Set == nil {
			return nil, nil, nil
		}
		for _, param := range cmd.Set.SetParams {
			key := param.Key
			value := unquoteIfQuoted(param.Value)
			op := param.Op
			// If no operator is specified, default to "="
			if op == "" {
				op = "="
			}
			if op == "=" {
				setMap[key] = value
			} else if op == "+=" {
				addMap[key] = value
			} else if op == "-=" {
				deleteMap[key] = value
			}
		}
	default:
		return nil, nil, nil
	}
	return setMap, addMap, deleteMap
}

func preParseGlobalFlags(args []string) []string {
	remainingArgs := []string{}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		var flagName, flagValue string
		var hasValueInSameArg bool

		if strings.HasPrefix(arg, "-") {
			if strings.Contains(arg, "=") {
				parts := strings.SplitN(arg, "=", 2)
				flagName = parts[0]
				flagValue = parts[1]
				hasValueInSameArg = true
			} else {
				flagName = arg
			}
		} else {
			remainingArgs = append(remainingArgs, arg)
			continue
		}

		switch flagName {
		case "-h", "--help":
			showHelp()
			os.Exit(0)
		case "-v", "-V", "--version":
			fmt.Println(util.Version())
			os.Exit(0)
		case "-J", "--json":
			FlagJson = true
		case "-C", "--config":
			if hasValueInSameArg {
				FlagConfigFilePath = flagValue
			} else if i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
				FlagConfigFilePath = args[i+1]
				i++
			}
		case "-p", "--plugin-config":
			if hasValueInSameArg {
				FlagPluginConfigFilePath = flagValue
			} else if i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
				FlagPluginConfigFilePath = args[i+1]
				i++
			}
		case "--force", "-f":
			FlagForce = true
		case "--partition-limit", "-P":
			FlagShowPartitionLimit = true
		default:
			remainingArgs = append(remainingArgs, arg)
		}
	}
	return remainingArgs
}

func unquoteIfQuoted(s string) string {
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'') {
			return s[1 : len(s)-1]
		}
	}
	return s
}

func getCmdStringByArgs(commandArgs []string) string {
	commandArgs = normalizeShowCommandArgs(commandArgs)

	var processedArgs []string
	for _, arg := range commandArgs {
		if arg == "" {
			processedArgs = append(processedArgs, "\"\"")
			continue
		}
		//  FlagFormat
		if strings.Contains(arg, "format") {
			// format: format=Name...   format Name...
			FlagFormat = arg[7:]
			continue
		}

		if strings.Contains(arg, "=") {
			parts := strings.SplitN(arg, "=", 2)
			key := parts[0]
			value := parts[1]

			if value == "" {
				processedArgs = append(processedArgs, key+"=\"\"")
				continue
			}
			if (strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'")) ||
				(strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"")) {
				processedArgs = append(processedArgs, arg)
			} else if strings.Contains(value, " ") {
				processedArgs = append(processedArgs, key+"="+strconv.Quote(value))
			} else {
				processedArgs = append(processedArgs, arg)
			}
		} else if strings.Contains(arg, " ") && !strings.HasPrefix(arg, "'") && !strings.HasPrefix(arg, "\"") {
			processedArgs = append(processedArgs, strconv.Quote(arg))
		} else {
			processedArgs = append(processedArgs, arg)
		}
	}
	cmdStr := strings.Join(processedArgs, " ")
	return cmdStr
}

func normalizeShowCommandArgs(commandArgs []string) []string {
	if len(commandArgs) < 3 || !strings.EqualFold(commandArgs[0], "show") {
		return commandArgs
	}

	if !strings.EqualFold(commandArgs[1], "transaction") {
		return commandArgs
	}

	if strings.EqualFold(commandArgs[2], "where") || !isTxnFilterArg(commandArgs[2]) {
		return commandArgs
	}

	normalizedArgs := make([]string, 0, len(commandArgs)+1)
	normalizedArgs = append(normalizedArgs, commandArgs[:2]...)
	normalizedArgs = append(normalizedArgs, "where")
	normalizedArgs = append(normalizedArgs, commandArgs[2:]...)
	return normalizedArgs
}

func isTxnFilterArg(arg string) bool {
	key := arg
	if strings.Contains(arg, "=") {
		parts := strings.SplitN(arg, "=", 2)
		key = parts[0]
	}

	switch strings.ToLower(key) {
	case "actor", "target", "action", "info", "starttime":
		return true
	default:
		return false
	}
}
