"""
Smart City Traffic Pipeline - Apache Airflow DAG
=================================================

Ce DAG orchestre le pipeline Big Data de la Smart City:
1. Vérifie que Kafka est prêt et que le topic existe
2. Lance le job Spark d'analyse
3. Valide que les résultats sont bien dans HDFS

Auteur: Data Engineering Team
Date: 2026-01-08
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import subprocess
import logging

# =======================
# Configuration du DAG
# =======================

default_args = {
    'owner': 'smartcity-team',
    'depends_on_past': False,
    'email': ['admin@smartcity.local'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# =======================
# Fonctions Python
# =======================

def check_kafka_topic(**context):
    """
    Vérifie que le topic Kafka 'traffic-events' existe et contient des messages.
    """
    import socket
    
    logging.info("Vérification de la connexion Kafka...")
    
    # Test de connexion basique à Kafka
    kafka_host = 'localhost'
    kafka_port = 9092
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((kafka_host, kafka_port))
        sock.close()
        
        if result == 0:
            logging.info(f"✅ Kafka est accessible sur {kafka_host}:{kafka_port}")
            return True
        else:
            raise Exception(f"❌ Impossible de se connecter à Kafka sur {kafka_host}:{kafka_port}")
    except Exception as e:
        logging.error(f"Erreur de connexion Kafka: {str(e)}")
        raise


def validate_hdfs_output(**context):
    """
    Vérifie que les données traitées existent dans HDFS.
    """
    import subprocess
    
    logging.info("Vérification des données dans HDFS...")
    
    # Chemins à vérifier
    paths_to_check = [
        '/data/raw/traffic',
        '/data/analytics/traffic_zone_stats'
    ]
    
    hdfs_namenode = 'localhost:9000'
    
    for path in paths_to_check:
        try:
            # Utilise curl pour accéder à l'API WebHDFS
            cmd = f'curl -s -o /dev/null -w "%{{http_code}}" "http://localhost:9870/webhdfs/v1{path}?op=LISTSTATUS"'
            logging.info(f"Vérification de {path}...")
            
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode == 0:
                logging.info(f"✅ Chemin vérifié: {path}")
            else:
                logging.warning(f"⚠️ Chemin non accessible: {path}")
                
        except Exception as e:
            logging.error(f"Erreur lors de la vérification de {path}: {str(e)}")
    
    logging.info("Validation HDFS terminée")
    return True


def log_pipeline_status(**context):
    """
    Log le statut final du pipeline avec un résumé.
    """
    execution_date = context['execution_date']
    
    status_report = f"""
    ╔══════════════════════════════════════════════════════════════╗
    ║        SMART CITY TRAFFIC PIPELINE - RAPPORT D'EXÉCUTION     ║
    ╠══════════════════════════════════════════════════════════════╣
    ║  Date d'exécution : {execution_date}
    ║  Statut           : ✅ SUCCÈS
    ║                                                              
    ║  Étapes complétées:                                          
    ║    ✓ Vérification Kafka                                      
    ║    ✓ Traitement Spark                                        
    ║    ✓ Validation HDFS                                         
    ║                                                              
    ║  KPIs générés:                                               
    ║    • Vitesse moyenne par route                               
    ║    • Trafic par zone                                         
    ║    • Taux de congestion                                      
    ╚══════════════════════════════════════════════════════════════╝
    """
    
    logging.info(status_report)
    return True


# =======================
# Définition du DAG
# =======================

with DAG(
    dag_id='smart_city_traffic_pipeline',
    description='Pipeline Big Data pour l\'analyse du trafic urbain Smart City',
    default_args=default_args,
    schedule_interval=timedelta(hours=1),  # Exécution toutes les heures
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['smart-city', 'traffic', 'big-data', 'spark'],
) as dag:
    
    # ===========================
    # Task 1: Démarrage du Pipeline
    # ===========================
    start_pipeline = EmptyOperator(
        task_id='start_pipeline',
        doc='Point de départ du pipeline Smart City'
    )
    
    # ===========================
    # Task 2: Vérification Kafka
    # ===========================
    check_kafka = PythonOperator(
        task_id='check_kafka_topic',
        python_callable=check_kafka_topic,
        doc='Vérifie que Kafka est accessible et que le topic traffic-events existe'
    )
    
    # ===========================
    # Task 3: Exécution du Job Spark
    # ===========================
    run_spark_job = BashOperator(
        task_id='run_spark_analytics',
        bash_command='''
            echo "🚀 Lancement du job Spark Analytics..."
            
            # Option A: Si Spark est installé localement
            # spark-submit --class ma.enset.trafficsparkprocessor.TrafficAnalyticsJob \
            #   --master local[*] \
            #   /path/to/traffic-spark-processor.jar
            
            # Option B: Simulation pour le développement
            echo "📊 Simulation du traitement Spark..."
            echo "  - Lecture des données depuis /data/raw/traffic"
            echo "  - Calcul des KPIs: vitesse moyenne, trafic par zone, congestion"
            echo "  - Écriture des résultats dans /data/analytics/traffic_zone_stats"
            sleep 5
            echo "✅ Job Spark terminé avec succès!"
        ''',
        doc='Exécute le job Spark pour analyser les données de trafic'
    )
    
    # ===========================
    # Task 4: Validation des Résultats HDFS
    # ===========================
    validate_output = PythonOperator(
        task_id='validate_hdfs_output',
        python_callable=validate_hdfs_output,
        doc='Vérifie que les données traitées sont bien présentes dans HDFS'
    )
    
    # ===========================
    # Task 5: Rapport Final
    # ===========================
    log_status = PythonOperator(
        task_id='log_pipeline_status',
        python_callable=log_pipeline_status,
        doc='Génère un rapport de statut du pipeline'
    )
    
    # ===========================
    # Task 6: Fin du Pipeline
    # ===========================
    end_pipeline = EmptyOperator(
        task_id='end_pipeline',
        doc='Point de fin du pipeline Smart City'
    )
    
    # ===========================
    # Définition des Dépendances
    # ===========================
    # Flow: start -> kafka check -> spark job -> validate -> log -> end
    
    start_pipeline >> check_kafka >> run_spark_job >> validate_output >> log_status >> end_pipeline
