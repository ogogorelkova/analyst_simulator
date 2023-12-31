# analyst_simulator

Это учебный проект, сделанный во время прохождения [Симулятора Аналитика](https://karpov.courses/simulator) от karpov.courses 

 ---

Задача проекта - настороить аналитику в стратрапе - социальной сети. 
Данные для анализа генерируются в реальном времени.

Что я сделала в этом проекте: 

1. **Проведение AA и AB тестирования** \
   В сервис была внедрена новая методика рекомендаций постов, и я проверила ее эффективность путем проведения АБ=теста, сперва убедившись на АА-тесте, что наше сплитование корректно.\
   [AA-test](EOG_ABtest1.ipynb) \
   [AB-test](EOG_ABtest2.ipynb) \
   [AB-test linearized method](EOG_ABtest3.ipynb) 

2. **Прогнозирование метрик** \
В сервисе был проведен флешмоб, который повлиял на основные метрики. При помощи метода casual impact я убедилась, что эффект от флешмоба - долгосрочный. \
   [Casual impact](EOG_7Prediction_1.ipynb) \

3. **ETL - pipeline** \
   Я настоила наполнение нового отчета ежедневными данными, автоматизировав его через Airflow \
   [ETL - daily report update](EOG_ETL_newreport.py)

4. **Автоматизация отчетности и система алертов** \
   Создала телеграм-бота, который ежедневно рассылает отчеты с основными метриками \
   [main metrics](eog_mainmetrics.py) \
   [all metrics](eog_allmetrics.py) \
   Настоила систему алертов в случае отклонения метрик от ожидаемого значения, которые также приходят через телеграм \
   [alerts](alert.py) 
   
 ---
### стек
 - Jupyter Notebook
 - GIT
 - Superset
 - ClickHouse
 - Airflow

