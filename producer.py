from tweepy import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
import config

producer = KafkaProducer(bootstrap_servers=config.SERVER_KAFKA)

class StdOutListener(StreamListener):
  def on_data(self, data):
    producer.send(config.TOPIC_NAME, str.encode(data))
    print(data)
    return True

  def on_error(self, status):
    print(status)
  
if __name__ == '__main__':
  auth = OAuthHandler(config.API_KEY, config.API_SECRET_KEY)
  auth.set_access_token(config.ACCESS_TOKEN, config.ACCESS_TOKEN_SECRET)
  listener  = StdOutListener()
  stream = Stream(auth, listener )

  # Setting para la busqueda
  tracks = config.TRACKS        # Palabras, usuarios, hastags a buscar
  location = config.LOCATION    # Ubicacion area de tuits
  languages = config.LANGUAGES  # Idioma de los tuis

  stream.filter(track=tracks, locations=location, languages=languages)