# ЭТАП 1: Сборка (Build)
# Берем образ с Maven и Java 21 для сборки кода
FROM maven:3.9.6-eclipse-temurin-21-alpine AS build
WORKDIR /app

# Копируем файл настроек проекта
COPY pom.xml .

# Сначала скачиваем зависимости (это ускорит сборку в будущем)
RUN mvn dependency:go-offline

# Копируем исходный код
COPY src ./src

# Собираем проект (создаем тот самый .jar файл)
RUN mvn clean package -DskipTests

# ЭТАП 2: Запуск (Run)
# Берем очень легкий образ только с Java для запуска
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# Копируем СОБРАННЫЙ файл из первого этапа (build) во второй
COPY --from=build /app/target/*.jar app.jar

EXPOSE 8080

# Команда запуска с профилем prod (как просил Аксамит)
ENTRYPOINT ["java", "-Dspring.profiles.active=prod", "-jar", "app.jar"]
