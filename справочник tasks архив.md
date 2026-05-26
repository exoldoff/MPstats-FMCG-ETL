TASKS = [
    # Маргарин
    {"mp": "oz", "path": "Продукты питания/Молочные продукты, сыры и яйца/Масло, маргарин и спред", "fbs": 1, "cat": "Маргарин", "filterModel": {
        "name": {"filterType": "text", "type": "contains", "filter": "Маргарин"}
    }},
    {"mp": "wb", "path": "Продукты", "fbs": 1, "cat": "Маргарин", "filterModel": {
        "name": {"filterType": "text", "type": "contains", "filter": "Маргарин"}
    }},
    {"mp": "ym", "path": "Продукты", "cat": "Маргарин", "filterModel": {
        "name": {"filterType": "text", "type": "contains", "filter": "Маргарин"}
    }},
    # Спред
    {"mp": "oz", "path": "Продукты питания", "fbs": 1, "cat": "Спред", "filterModel": {
        "name": {"filterType": "text", "type": "contains", "filter": "Спред"}
    }},
    {"mp": "wb", "path": "Продукты", "fbs": 1, "cat": "Спред ", "filterModel": {
        "name": {"filterType": "text", "type": "contains", "filter": "Спред"}
    }},
    # Топленая смесь — OR по двум фразам (| в справочнике)
    {"mp": "oz", "path": "Продукты питания", "fbs": 1, "cat": "Топленая смесь", "filterModel": {
        "name": {
            "filterType": "text",
            "operator": "OR",
            "condition1": {"filterType": "text", "type": "contains", "filter": "топленая смесь"},
            "condition2": {"filterType": "text", "type": "contains", "filter": "смесь топленая"},
        }
    }},
    {"mp": "wb", "path": "Продукты", "fbs": 1, "cat": "Топленая смесь", "filterModel": {
        "name": {
            "filterType": "text",
            "operator": "OR",
            "condition1": {"filterType": "text", "type": "contains", "filter": "топленая смесь"},
            "condition2": {"filterType": "text", "type": "contains", "filter": "смесь топленая"},
        }
    }},
    # Вега-спред
    {"mp": "oz", "path": "Продукты питания", "fbs": 1, "cat": "Вега-спред", "filterModel": {
        "name": {"filterType": "text", "type": "contains", "filter": "De olio"}
    }},
    {"mp": "wb", "path": "Продукты/Бакалея/Масла, соусы", "fbs": 1, "cat": "Вега-спред", "filterModel": {
        "name": {
            "filterType": "text",
            "operator": "OR",
            "condition1": {"filterType": "text", "type": "contains", "filter": "De Olio"},
            "condition2": {"filterType": "text", "type": "contains", "filter": "Вега"},
        }
    }},
    {"mp": "ym", "path": "Продукты/Диетическое и лечебное питание/Продукты на растительной основе/Растительные молочные продукты", "cat": "Вега-спред", "filterModel": {
        "name": {"filterType": "text", "type": "contains", "filter": "Вегамасло"}
    }},
    # Кокосовое масло — OR
    {"mp": "oz", "path": "Продукты питания", "fbs": 1, "cat": "Кокосовое масло", "filterModel": {
        "name": {
            "filterType": "text",
            "operator": "OR",
            "condition1": {"filterType": "text", "type": "contains", "filter": "Кокосовое масло"},
            "condition2": {"filterType": "text", "type": "contains", "filter": "Масло кокосовое"},
        }
    }},
    {"mp": "wb", "path": "Продукты", "fbs": 1, "cat": "Кокосовое масло", "filterModel": {
        "name": {
            "filterType": "text",
            "operator": "OR",
            "condition1": {"filterType": "text", "type": "contains", "filter": "Кокосовое масло"},
            "condition2": {"filterType": "text", "type": "contains", "filter": "Масло кокосовое"},
        }
    }},
    {"mp": "ym", "path": "Продукты", "cat": "Кокосовое масло", "filterModel": {
        "name": {
            "filterType": "text",
            "operator": "OR",
            "condition1": {"filterType": "text", "type": "contains", "filter": "Кокосовое масло"},
            "condition2": {"filterType": "text", "type": "contains", "filter": "Масло кокосовое"},
        }
    }},
    # Сливочное масло
    {"mp": "oz", "path": "Ozon fresh/Молочное и яйца/Молоко, масло и яйца/Масло, маргарин", "fbs": 1, "cat": "Сливочное масло", "filterModel": {
        "name": {"filterType": "text", "type": "contains", "filter": "Масло"}
    }},
    {"mp": "wb", "path": "Продукты/Молочные продукты и яйца", "fbs": 1, "cat": "Сливочное масло", "filterModel": {
        "name": {
            "filterType": "text",
            "operator": "AND",
            "condition1": {"filterType": "text", "type": "contains", "filter": "масло сливочное"},
            "condition2": {"filterType": "text", "type": "notContains", "filter": "топленое"},
        }
    }},
    {"mp": "ym", "path": "Продукты/Молочная гастрономия/Масло и маргарин/Сливочное масло", "cat": "Сливочное масло"},
    # Продукт раст.-слив.
    {"mp": "oz", "path": "Продукты питания/Молочные продукты, сыры и яйца/Масло, маргарин и спред", "fbs": 1, "cat": "Продукт раст.-слив.", "filterModel": {
        "name": {"filterType": "text", "type": "contains", "filter": "продукт"}
    }},
]
# Мыло
    {"mp": "oz", "path": "Красота и здоровье/Уход за телом", "fbs": 1, "cat": "Мыло", "filterModel": {
        "name": {"filterType": "text", "type": "contains", "filter": "мыло"}
    }},
    {"mp": "wb", "path": "Красота/", "fbs": 1, "cat": "Мыло", "filterModel": {
        "name": {"filterType": "text", "type": "contains", "filter": "мыло"}
    }},
    {"mp": "ym", "path": "Товары для красоты/Косметика, парфюмерия и уход", "cat": "Мыло", "filterModel": {
        "name": {"filterType": "text", "type": "contains", "filter": "мыло"}
    }},
    # Мыло хозяйственное
    {"mp": "oz", "path": "Бытовая химия и гигиена/Бытовая химия", "fbs": 1, "cat": "Мыло хозяйственное", "filterModel": {
        "name": {
            "filterType": "text",
            "operator": "AND",
            "condition1": {"filterType": "text", "type": "contains", "filter": "мыло"},
            "condition2": {"filterType": "text", "type": "contains", "filter": "хозяйственное"},
        }
    }},
    {"mp": "wb", "path": "Дом/", "fbs": 1, "cat": "Мыло хозяйственное", "filterModel": {
        "name": {
            "filterType": "text",
            "operator": "AND",
            "condition1": {"filterType": "text", "type": "contains", "filter": "мыло"},
            "condition2": {"filterType": "text", "type": "contains", "filter": "хозяйственное"},
        }
    }},
    {"mp": "ym", "path": "Товары для дома/Бытовая химия", "cat": "Мыло хозяйственное", "filterModel": {
        "name": {
            "filterType": "text",
            "operator": "AND",
            "condition1": {"filterType": "text", "type": "contains", "filter": "мыло"},
            "condition2": {"filterType": "text", "type": "contains", "filter": "хозяйственное"},
        }
    }},