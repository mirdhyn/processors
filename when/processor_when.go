// void just do nothing with events, so usefull !
package when

import (
	"fmt"
	"strings"

	"github.com/veino/processors"
	"github.com/veino/veino"
	"github.com/vjeantet/govaluate"
)

type processor struct {
	processors.Base

	opt                 *options
	compiledExpressions map[int]*govaluate.EvaluableExpression
}

type options struct {
	Expressions map[int]string
}

func New() veino.Processor {
	return &processor{
		compiledExpressions: map[int]*govaluate.EvaluableExpression{},
		opt:                 &options{},
	}
}

func (p *processor) Configure(ctx veino.ProcessorContext, conf map[string]interface{}) error {
	return p.ConfigureAndValidate(ctx, conf, p.opt)
}

// comparison operators
// equality: ==, !=, <, >, <=, >=
// regexp: =~, !~
// inclusion: in, not in

// boolean operators
// and, or, nand, xor

// unary operators
// !

// Expressions can be long and complex. Expressions can contain other expressions,
// you can negate expressions with !, and you can group them with parentheses (...).

// 127.0.0.1 - - [11/Dec/2013:00:01:45 -0800] "GET /xampp/status.php HTTP/1.1" 200 3891 "http://cadenza/xampp/navi.php" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:25.0) Gecko/20100101 Firefox/25.0"

func (p *processor) Receive(e veino.IPacket) error {
	for order := 0; order < len(p.opt.Expressions); order++ {
		expressionValue := p.opt.Expressions[order]

		result, err := p.assertExpressionWithFields(order, expressionValue, e)
		if err != nil {
			p.Logger.Printf("When processor evaluation error : %s\n", err.Error())
			continue
		}

		if result {
			p.Send(e, order)
			break
		}
	}
	return nil
}

// With Knetic/govaluate

func (p *processor) assertExpressionWithFields(index int, expressionValue string, e veino.IPacket) (bool, error) {
	expression, err := p.cacheExpression(index, expressionValue)
	if err != nil {
		return false, fmt.Errorf("conditional expression error : %s", err.Error())
	}
	parameters := make(map[string]interface{})
	for _, v := range expression.Tokens() {
		if v.Kind == govaluate.VARIABLE {
			paramValue, err := e.Fields().ValueForPath(v.Value.(string))
			if err != nil {
				return false, fmt.Errorf("conditional field not found : %s", err.Error())
			}

			parameters[v.Value.(string)] = paramValue
		}
	}
	result, err := expression.Evaluate(parameters)

	return result.(bool), err
}

func (p *processor) cacheExpression(index int, expressionValue string) (*govaluate.EvaluableExpression, error) {
	if e, ok := p.compiledExpressions[index]; ok {
		return e, nil
	}

	expressionValue = strings.Replace(expressionValue, `][`, `.`, -1)
	expression, err := govaluate.NewEvaluableExpression(expressionValue)
	if err != nil {
		return nil, err
	}
	p.compiledExpressions[index] = expression

	return expression, nil
}
