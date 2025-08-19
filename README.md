```mermaid

erDiagram
  ШКОЛЫ {
    bigint id PK
    text название
    text адрес
  }

  УЧЕБНЫЕ_ГОДА {
    bigint id PK
    bigint школа_id FK
    text наименование
    date дата_начала
    date дата_окончания
  }

  ПЕРИОДЫ {
    bigint id PK
    bigint учебный_год_id FK
    text наименование
    date дата_начала
    date дата_окончания
  }

  УЧИТЕЛЯ {
    bigint id PK
    bigint школа_id FK
    bigint классрук_id FK
    text имя
    text фамилия
    text email
    text телефон
    date дата_приема
    boolean активен
  }

  КЛАССЫ {
    bigint id PK
    bigint учебный_год_id FK
    bigint школа_id FK

    text название
    smallint номер_класса
    boolean активен
    text тип
    bigint классрук_id FK
  }

  ПРЕДМЕТЫ {
    bigint id PK
    text название
    text краткое_название
  }

  УЧИТЕЛЬ_ПРЕДМЕТ {
    bigint id PK
    bigint учитель_id FK
    bigint предмет_id FK
  }

  ДИСЦИПЛИНЫ_КЛАССА {
    bigint id PK
    bigint класс_id FK
    bigint предмет_id FK
    bigint учитель_id FK
    bigint период_id FK
    text тип
    numeric часов_в_нед
  }

  УЧЕНИКИ {
    bigint id PK
    bigint пользователь_id FK
    text имя
    text фамилия
    date дата_рождения
    date дата_зачисления
    text статус
  }

  УЧЕНИКИ_КЛАССЫ {
    bigint id PK
    bigint ученики_id FK
    bigint класс_id FK
    text роль
    boolean активен
    date дата_начала
    date дата_окончания
  }
  
  МАКЕТ_РАСПИСАНИЯ {
    bigint id PK
    bigint период_id FK
    smallint номер_урока
    time время_начала
    time время_конца
    smallint длительность_мин
    text день_недели
    boolean активен
  }

  УРОКИ {
    bigint id PK
    bigint макет_id FK
    bigint дисциплина_класса_id FK
    date дата
    text тема
    bigint дз_id FK
    bigint кабинет_id FK
    boolean проведен
  }


  ОЦЕНКИ {
    bigint id PK
    bigint ученик_id FK
    bigint урок_id FK
    bigint учитель_id FK
    smallint оценка
    smallint вес 
    text комментарий
    timestamp создано
  }
  ДЗ {
    bigint id PK
    bigint урок_id FK
    text тема
    text описание
    timestamp выдано
    timestamp дедлайн
  }

  СДАЧИ_ДЗ {
    bigint id PK
    bigint дз_id FK
    bigint ученик_id FK
    timestamp сдано
    text контент_uri
    text комментарий
  }

  ПОЛЬЗОВАТЕЛИ {
    bigint id PK
    text email
    text телефон
    text пароль_хеш
    boolean активен
    timestamp создано
  }

  РОЛИ {
    bigint id PK
    text код
    text название
  }

  РОЛИ_ПОЛЬЗОВАТЕЛЯ {
    bigint пользователь_id FK
    bigint роль_id FK
  }

 

  РОДИТЕЛИ {
    bigint id PK
    bigint пользователь_id FK
    text имя
    text фамилия
    text email
    text телефон
  }

  УЧЕНИК_РОДИТЕЛЬ {
    bigint ученик_id FK
    bigint родитель_id FK
    text отношение
    boolean основной
  }



  КАБИНЕТЫ {
    bigint id PK
    bigint школа_id FK
    text код
    smallint вместимость
  }



  

  ОПРАВД_ДОКУМЕНТЫ {
    bigint id PK
    bigint ученик_id FK
    date дата_выдачи
    date действует_с
    date действует_по
    text тип
    text файл_uri
  }
  ИТОГОВЫЕ_ОЦЕНКИ {
    bigint id PK
    bigint ученик_id FK
    bigint период_id FK
    bigint предмет_id FK
    smallint оценка
    text комментарий
  }

  ПОСЕЩАЕМОСТЬ {
    bigint id PK
    bigint урок_id FK
    bigint ученик_id FK
    text статус
    text причина
    bigint документ_id FK
  }

  ИТОГОВЫЕ_ОЦЕНКИ {
    bigint id PK
    bigint ученик_id FK
    bigint урок_id FK
    bigint период_id FK 
    bigint учебный_год_id FK 
    text тип            
    smallint оценка
    text комментарий
    timestamp создано
  }


  
  
 
  ШКОЛЫ ||--o{ УЧЕБНЫЕ_ГОДА : ""
  УЧЕБНЫЕ_ГОДА ||--o{ ПЕРИОДЫ : ""
  УЧЕБНЫЕ_ГОДА ||--o{ КЛАССЫ : ""
  ШКОЛЫ ||--o{ КЛАССЫ : ""
  ШКОЛЫ ||--o{ УЧИТЕЛЯ : ""
  УЧИТЕЛЯ ||--|| КЛАССЫ : ""
  ПРЕДМЕТЫ ||--o{ УЧИТЕЛЬ_ПРЕДМЕТ : ""
  УЧИТЕЛЯ ||--o{ УЧИТЕЛЬ_ПРЕДМЕТ : ""
  КЛАССЫ ||--o{ ДИСЦИПЛИНЫ_КЛАССА : ""
  ПРЕДМЕТЫ ||--o{ ДИСЦИПЛИНЫ_КЛАССА : ""
  УЧИТЕЛЯ ||--o{ ДИСЦИПЛИНЫ_КЛАССА : ""
  ПЕРИОДЫ ||--o{ ДИСЦИПЛИНЫ_КЛАССА : ""
  УЧЕНИКИ ||--o{ УЧЕНИКИ_КЛАССЫ : ""
  КЛАССЫ ||--o{ УЧЕНИКИ_КЛАССЫ : ""
  ПЕРИОДЫ ||--o{ МАКЕТ_РАСПИСАНИЯ : ""
  МАКЕТ_РАСПИСАНИЯ ||--o{ УРОКИ : ""
  ДИСЦИПЛИНЫ_КЛАССА ||--o{ УРОКИ : ""
  КАБИНЕТЫ ||--o{ УРОКИ : ""
  УРОКИ ||--o{ ОЦЕНКИ : ""
  УЧЕНИКИ ||--o{ ОЦЕНКИ : ""
  УЧИТЕЛЯ ||--o{ ОЦЕНКИ : ""
  УРОКИ ||--o{ ДЗ : ""
  ДЗ ||--o{ СДАЧИ_ДЗ : ""
  УЧЕНИКИ ||--o{ СДАЧИ_ДЗ : ""
  ПОЛЬЗОВАТЕЛИ ||--o{ УЧЕНИКИ : ""
  ПОЛЬЗОВАТЕЛИ ||--o{ РОДИТЕЛИ : ""
  РОДИТЕЛИ ||--o{ УЧЕНИК_РОДИТЕЛЬ : ""
  УЧЕНИКИ ||--o{ УЧЕНИК_РОДИТЕЛЬ : ""
  ПОЛЬЗОВАТЕЛИ ||--o{ РОЛИ_ПОЛЬЗОВАТЕЛЯ : ""
  РОЛИ ||--o{ РОЛИ_ПОЛЬЗОВАТЕЛЯ : ""
  УЧЕНИКИ ||--o{ ОПРАВД_ДОКУМЕНТЫ : ""
  УЧЕНИКИ ||--o{ ПОСЕЩАЕМОСТЬ : ""
  УРОКИ ||--o{ ПОСЕЩАЕМОСТЬ : ""
  ОПРАВД_ДОКУМЕНТЫ ||--o{ ПОСЕЩАЕМОСТЬ : ""
  УЧЕНИКИ ||--o{ ИТОГОВЫЕ_ОЦЕНКИ : ""
  УРОКИ ||--o{ ИТОГОВЫЕ_ОЦЕНКИ : ""
  ПЕРИОДЫ ||--o{ ИТОГОВЫЕ_ОЦЕНКИ : ""
  УЧЕБНЫЕ_ГОДА ||--o{ ИТОГОВЫЕ_ОЦЕНКИ : ""


 
