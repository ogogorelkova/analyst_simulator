# analyst_simulator

Это учебный проект, сделанный во время прохождения [Симулятора Аналитика](https://karpov.courses/simulator) от karpov.courses 

 ---

Задача проекта - настороить аналитику в стратрапе - социальной сети. 
Данные для анализа генерируются в реальном времени.

Что я сделала в этом проекте: 

1. **Предварительный анализ данных:** \
   [дашборд c основными метриками](https://superset.lab.karpov.courses/superset/dashboard/4103/) \
   [дашборд c оперативными метриками](https://superset.lab.karpov.courses/superset/dashboard/4108/)

2. **Проведение AA и AB тестирования** \
   В сервис была внедрена новая методика рекомендаций постов, и я проверила ее эффективность путем проведения АБ=теста, сперва убедившись на АА-тесте, что наше сплитование корректно.\
   [AA-test](EOG_ABtest1.ipynb) \
   [AB-test](EOG_ABtest2.ipynb) \
   [AB-test linearized method](EOG_ABtest3.ipynb) 

4. **Прогнозирование метрик** \
В сервисе был проведен флешмоб, который повлиял на основные метрики. При помощи метода casual impact я убедилась, что эффект от флешмоба - долгосрочный. \
   [Casual impact](EOG_7Prediction_1.ipynb) \
Также я спрогнозировала нагрузку на сервер, так как количество пользователей сервиса растет \
  TBD: [Activity prediction] 

5. **ETL - pipeline** \
   Я настоила наполнение нового отчета ежедневными данными, автоматизировав его через Airflow
   [ETL](EOG_ETL_newreport.py)

6. **Автоматизация отчетности и система алертов** \
   Создала телеграм-бота, который ежедневно рассылает отчеты с основными метриками \
   TBD: [main metrics] \
   TBD: [all metrics] \
   Настоила систему алертов в случае отклонения метрик от ожидаемого значения, которые также приходят через телеграм \
   TBD: [alerts] 
   
 ---
### стек
 - Jupyter Notebook
 - GIT
 - Superset
 - ClickHouse
 - Airflow

