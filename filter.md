Фильтрация данных
    

// текстовые фильтры используют след. модель
interface TextFilterModel {
    // равен 'text'
    filterType: string;

    // один из вариантов фильтра, например «равно»
    type: string;

    // текстовое значение, связанное с фильтром.
    // это необязательно, так как фильтры могут не иметь значения
    filter?: string;
}
    

    

// числовые фильтры используют след. модель
interface NumberFilterModel {
    // равен 'number'
    filterType: string;

    // один из вариантов фильтра, например «равно»
    type: string;

    // числовые значения
    // это необязательно, так как фильтры могут не иметь значения
    // фильтр диапазона имеет два значения (от и до).
    filter?: number;
    filterTo?: number;
}
    

    

// фильтры даты используют след. модель
interface NumberFilterModel {
    // равен 'date'
    filterType: string;

    // один из вариантов фильтра, например «равно»
    type: string;

    // текстовые значения
    // это необязательно, так как фильтры могут не иметь значения
    // тип - строка, а формат - всегда ГГГГ-ММ-ДД, например. 2019-05-24
    // фильтр диапазона имеет два значения (от и до).
    filter?: string;
    filterTo?: string;
}
    

Примеры модели фильтра:

    

//числовой фильтр с одним условием
balance: {
    filterType: 'number',
    type: 'lessThan',
    filter: 35
}
    

    

//числовой фильтр с одним условием и диапазоном
balance: {
    filterType: 'number',
    type: 'inRange',
    filter: 35,
    filterTo: 40
};
    

Если в фильтре задано как Условие 1, так и Условие 2, создаются два экземпляра модели, которые заключаются в Комбинированную модель. Комбинированная модель выглядит следующим образом:

    

// Фильтр, объединяющий два условия
// M - это либо TextFilterModel, NumberFilterModel, либо DateFilterModel
interface ICombinedSimpleModel<M> {
    // тип фильтра: date, number or text
    filterType: string;

    // одно из условий 'AND' или 'OR'
    operator: string;

    // два экземпляра модели фильтра
    condition1: M;
    condition2: M;
}
    

Пример модели фильтра с двумя условиями выглядит следующим образом:

    

balance: {
    filterType: 'number',
    operator: 'OR'
    condition1: {
        filterType: 'number',
        type: 'equals',
        filter: 18
    },
    condition2: {
        filterType: 'number',
        type: 'equals',
        filter: 19
    }
};
    

Типы фильтров
Параметр	Значение	поддерживаемы типы
Равно	equals	text, number, date
Не равно	notEqual	text, number, date
Содержит	contains	text
Не содержит	notContains	text
Начинается с	startsWith	text
Заканчивается	endsWith	text
Меньше, чем	lessThan	number, date
Меньше или равно	lessThanOrEqual	number
Больше чем	greaterThan	number, date
Больше или равно	greaterThanOrEqual	number
В диапазоне	inRange	number, date