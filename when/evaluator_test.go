package when

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/veino/runtime/testutils"
	"github.com/veino/veino"
	"github.com/vjeantet/govaluate"
)

func getTestEvent() veino.IPacket {
	m := map[string]interface{}{
		"testString":  "true",
		"testYes":     "yes",
		"testY":       "y",
		"testNo":      "no",
		"testN":       "n",
		"test1String": "1",
		"test1Int":    1,
		"test0String": "0",
		"test0Int":    0,
		"testBool":    true,
		"testInt":     4,
		"testInt3":    3,
		"way":         "SEND",
		"name":        "Valere",
		"tags": []string{
			"mytag",
			"_grokparsefailure",
			"_dateparsefailure",
		},
		"location": map[string]interface{}{
			"city":    "Paris",
			"country": "France",
		},
	}

	return testutils.NewTestEvent("test", "test", m)

}

func TestBasicLogicalStringEreg(t *testing.T) {
	event := getTestEvent()
	expression := "[way] =~ /(RECEIVE|SEND)/"
	p := &processor{compiledExpressions: map[int]*govaluate.EvaluableExpression{}}
	result, err := p.assertExpressionWithFields(0, expression, event)
	assert.Nil(t, err, "err is not nil")
	assert.True(t, result)
}

func TestBasicLogicalINStringSlice(t *testing.T) {
	event := getTestEvent()
	expression := `"_grokparsefailure" in [tags]`
	p := &processor{compiledExpressions: map[int]*govaluate.EvaluableExpression{}}
	result, err := p.assertExpressionWithFields(0, expression, event)
	assert.Nil(t, err, "err is not nil")
	assert.True(t, result)

	expression = `"grokparsefailure" in [tags]`
	result, err = p.assertExpressionWithFields(1, expression, event)
	assert.Nil(t, err, "err is not nil")
	assert.False(t, result)
}

func TestBasicLogicalStringNOTINSliceNotPresent(t *testing.T) {
	event := getTestEvent()
	expression := `"_mumu" not in [tags]`
	p := &processor{compiledExpressions: map[int]*govaluate.EvaluableExpression{}}
	result, err := p.assertExpressionWithFields(0, expression, event)
	assert.Nil(t, err, "err is not nil")
	assert.True(t, result)

	expression = `"_grokparsefailure" not in [tags]`
	result, err = p.assertExpressionWithFields(1, expression, event)
	assert.Nil(t, err, "err is not nil")
	assert.False(t, result)
}

func TestBasicLogicalStringEquality(t *testing.T) {
	event := getTestEvent()
	expression := "[testString] == \"true\""

	p := &processor{compiledExpressions: map[int]*govaluate.EvaluableExpression{}}
	result, err := p.assertExpressionWithFields(0, expression, event)
	assert.Nil(t, err, "err is not nil")
	assert.True(t, result)
}

func TestBasicLogicalStringEquality2(t *testing.T) {
	event := getTestEvent()
	expression := "[testString] == \"true\""

	p := &processor{compiledExpressions: map[int]*govaluate.EvaluableExpression{}}
	result, err := p.assertExpressionWithFields(0, expression, event)
	assert.Nil(t, err, "err is not nil")
	assert.True(t, result)
}

func TestBasicLogicalBooleanEquality(t *testing.T) {
	event := getTestEvent()
	expression := `[location][city] == "Paris"`

	p := &processor{compiledExpressions: map[int]*govaluate.EvaluableExpression{}}
	result, err := p.assertExpressionWithFields(0, expression, event)
	assert.Nil(t, err, "err is not nil")
	assert.True(t, result)
}

func TestBasicLogicalIntEquality(t *testing.T) {
	event := getTestEvent()
	expression := "[testInt] == 4"

	p := &processor{compiledExpressions: map[int]*govaluate.EvaluableExpression{}}
	result, err := p.assertExpressionWithFields(0, expression, event)
	assert.Nil(t, err, "err is not nil")
	assert.True(t, result)
}

func TestBasicLogicalIntGTFields(t *testing.T) {
	event := getTestEvent()
	expression := "[testInt] > [testInt3]"

	p := &processor{compiledExpressions: map[int]*govaluate.EvaluableExpression{}}
	result, err := p.assertExpressionWithFields(0, expression, event)
	assert.Nil(t, err, "err is not nil")
	assert.True(t, result)
}

func TestVariableNotSet(t *testing.T) {
	event := getTestEvent()
	expression := "[testUnk] > 3"
	p := &processor{compiledExpressions: map[int]*govaluate.EvaluableExpression{}}
	_, err := p.assertExpressionWithFields(0, expression, event)
	assert.NotNil(t, err, "err is not nil")
}

func TestExpressionBRoker(t *testing.T) {
	event := getTestEvent()
	expression := "[testUnk > 3"
	p := &processor{compiledExpressions: map[int]*govaluate.EvaluableExpression{}}
	_, err := p.assertExpressionWithFields(0, expression, event)
	assert.NotNil(t, err, "err is not nil")
}
