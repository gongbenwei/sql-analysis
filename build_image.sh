#构建镜像并且推送到镜像表
mvn clean package docker:build -DpushImage
