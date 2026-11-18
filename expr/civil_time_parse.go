package expr

type civilFields struct{ year, month, day, hour, minute, second, nano int }

func parseCivilText(s string) (civilFields, error) {
	var c civilFields
	if len(s) < 19 || len(s) > 29 {
		return c, ErrCivilSyntax
	}
	if s[4] != '-' || s[7] != '-' || s[10] != 'T' || s[13] != ':' || s[16] != ':' {
		return c, ErrCivilSyntax
	}
	number := func(a, b int) (int, bool) {
		n := 0
		for i := a; i < b; i++ {
			if s[i] < '0' || s[i] > '9' {
				return 0, false
			}
			n = n*10 + int(s[i]-'0')
		}
		return n, true
	}
	spans := [6][2]int{{0, 4}, {5, 7}, {8, 10}, {11, 13}, {14, 16}, {17, 19}}
	values := [6]int{}
	for i, p := range spans {
		n, ok := number(p[0], p[1])
		if !ok {
			return c, ErrCivilSyntax
		}
		values[i] = n
	}
	c = civilFields{year: values[0], month: values[1], day: values[2], hour: values[3], minute: values[4], second: values[5]}
	if c.month < 1 || c.month > 12 || c.day < 1 || c.hour > 23 || c.minute > 59 || c.second > 59 {
		return civilFields{}, ErrCivilSyntax
	}
	days := [12]int{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
	if c.year%4 == 0 && (c.year%100 != 0 || c.year%400 == 0) {
		days[1] = 29
	}
	if c.day > days[c.month-1] {
		return civilFields{}, ErrCivilSyntax
	}
	if len(s) > 19 {
		if len(s) < 21 || s[19] != '.' {
			return civilFields{}, ErrCivilSyntax
		}
		n, ok := number(20, len(s))
		if !ok {
			return civilFields{}, ErrCivilSyntax
		}
		for i := len(s) - 20; i < 9; i++ {
			n *= 10
		}
		c.nano = n
	}
	return c, nil
}
