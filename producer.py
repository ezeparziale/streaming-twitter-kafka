from tweepy import Stream
from kafka import KafkaProducer
import config

producer = KafkaProducer(bootstrap_servers=config.SERVER_KAFKA)

class StdOutListener(Stream):
  def on_data(self, data):
    producer.send(config.TOPIC_NAME, data)
    print(data)
    return True

  def on_error(self, status):
    print(status)
  
if __name__ == '__main__':
  stream = StdOutListener(config.API_KEY, config.API_SECRET_KEY, config.ACCESS_TOKEN, config.ACCESS_TOKEN_SECRET )

  # Setting para la busqueda
  tracks = config.TRACKS        # Palabras, usuarios, hastags a buscar
  location = config.LOCATION    # Ubicacion area de tuits
  languages = config.LANGUAGES  # Idioma de los tuis

  stream.filter(track=tracks, locations=location, languages=languages)