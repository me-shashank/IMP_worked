#! /bin/bash


ENV=ING_ENV_VALUE


mkdir -p /etc/secr_dir

KMS_KEY="KMS_KEY_VALUE"

QA_CIPHER_TEXT_SOURCE="QA_CIPHER_TEXT_SOURCE_VALUE"
PROD_CIPHER_TEXT_SOURCE="PROD_CIPHER_TEXT_SOURCE_VALUE"
DEV_CIPHER_TEXT_SOURCE="DEV_CIPHER_TEXT_SOURCE_VALUE"
CIPHER_TEXT_LANDING_BUCKET="CIPHER_TEXT_LANDING_BUCKET_VALUE"
CIPHER_TEXT_PUBSUB="CIPHER_TEXT_PUBSUB_VALUE"
CIPHER_TEXT_AUDITDB="CIPHER_TEXT_AUDITDB_VALUE"
CIPHER_TEXT_TRIGGER_BUCKET="CIPHER_TEXT_TRIGGER_BUCKET_VALUE"


echo "########## Initializing the startup script...##########"

CIPHER_TEXT_SOURCE="$PROD_CIPHER_TEXT_SOURCE"

if [ $ENV == "QA" ]; then
    CIPHER_TEXT_SOURCE="$QA_CIPHER_TEXT_SOURCE"
fi

if [ $ENV == "DEV" ]; then
    CIPHER_TEXT_SOURCE="$DEV_CIPHER_TEXT_SOURCE"
fi

echo "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"
echo "########## Get Service TOKEN from GCP...########## "

TOKEN=$(curl --silent --header "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token)
echo "TOKEN: $TOKEN"

ACCESS=$(echo ${TOKEN} | grep --extended-regexp --only-matching "(ya29.[0-9a-zA-Z._-]*)")
echo "ACCESS: $ACCESS"


echo "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"
echo "########## Extracting: Encoded cipher text from KMS...##########"

retry_count=0
while [[ "$retry_count" -lt 3 ]]; do

RESULT_SOURCE=$(curl -s -X POST -H "Authorization: Bearer ${ACCESS}" -H "Content-Type:application/json" -d "{\"ciphertext\": \"${CIPHER_TEXT_SOURCE}\"}" https://cloudkms.googleapis.com/v1/projects/${KMS_KEY}:decrypt)
BASE64_SEC_SOURCE=$( echo ${RESULT_SOURCE} | sed 's|,|\n|g' | grep -w "plaintext" | cut -d ":" -f2- | tr -d '"' )

  if [[ $BASE64_SEC_SOURCE == '' ]]; then
    if [[ $retry_count -lt 3 ]]; then
      echo "BASE64_SEC_SOURCE is coming as EMPTY, in attempt $retry_count!!! again try after 5 seconds"
      echo "RESULT_SOURCE: $RESULT_SOURCE"
      echo "BASE64_SEC_SOURCE: $BASE64_SEC_SOURCE"
      sleep 5
      continue
    else
      echo "BASE64_SEC_SOURCE is EMPTY, Maximum attempt exceeded: $retry_count, exit from loop!!!"
      exit 1
    fi
  else
    echo "Successfully collected CipherText from KMS on $retry_count try, for:"
    echo "RESULT_SOURCE: $RESULT_SOURCE"
    echo "BASE64_SEC_SOURCE: $BASE64_SEC_SOURCE"
    break
  fi
  (( retry_count+=1 ))

done


retry_count=0
while [[ "$retry_count" -lt 3 ]]; do

RESULT_AUDITDB=$(curl -s -X POST -H "Authorization: Bearer ${ACCESS}" -H "Content-Type:application/json" -d "{\"ciphertext\": \"${CIPHER_TEXT_AUDITDB}\"}" https://cloudkms.googleapis.com/v1/projects/${KMS_KEY}:decrypt)
BASE64_SEC_AUDITDB=$( echo ${RESULT_AUDITDB} | sed 's|,|\n|g' | grep -w "plaintext" | cut -d ":" -f2- | tr -d '"' )

  if [[ $BASE64_SEC_AUDITDB == '' ]]; then

    if [[ $retry_count -lt 3 ]]; then
      echo "BASE64_SEC_AUDITDB is coming as EMPTY, in attempt $retry_count!!! again try after 5 seconds"
      echo "RESULT_AUDITDB: $RESULT_AUDITDB"
      echo "BASE64_SEC_AUDITDB: $BASE64_SEC_AUDITDB"
      sleep 5
      continue
    else
      echo "BASE64_SEC_AUDITDB is EMPTY, Maximum attempt exceeded: $retry_count, exit from loop!!!"
      exit 1
    fi
  else
    echo "Successfully collected CipherText from KMS on $retry_count try, for:"
    echo "RESULT_AUDITDB: $RESULT_AUDITDB"
    echo "BASE64_SEC_AUDITDB: $BASE64_SEC_AUDITDB"
    break
  fi
  (( retry_count+=1 ))

done


retry_count=0
while [[ "$retry_count" -lt 3 ]]; do

RESULT_LANDING_BUCKET=$(curl -s -X POST -H "Authorization: Bearer ${ACCESS}" -H "Content-Type:application/json" -d "{\"ciphertext\": \"${CIPHER_TEXT_LANDING_BUCKET}\"}" https://cloudkms.googleapis.com/v1/projects/${KMS_KEY}:decrypt)
BASE64_SEC_LANDING_BUCKET=$( echo ${RESULT_LANDING_BUCKET} | sed 's|,|\n|g' | grep -w "plaintext" | cut -d ":" -f2- | tr -d '"' )

  if [[ $BASE64_SEC_LANDING_BUCKET == '' ]]; then

    if [[ $retry_count -lt 3 ]]; then
      echo "BASE64_SEC_LANDING_BUCKET is coming as EMPTY, in attempt $retry_count!!! again try after 5 seconds"
      echo "RESULT_LANDING_BUCKET: $RESULT_LANDING_BUCKET"
      echo "BASE64_SEC_LANDING_BUCKET: $BASE64_SEC_LANDING_BUCKET"
      sleep 5
      continue
    else
      echo "BASE64_SEC_LANDING_BUCKET is EMPTY, Maximum attempt exceeded: $retry_count, exit from loop!!!"
      exit 1
    fi
  else
    echo "Successfully collected CipherText from KMS on $retry_count try, for:"
    echo "RESULT_LANDING_BUCKET: $RESULT_LANDING_BUCKET"
    echo "BASE64_SEC_LANDING_BUCKET: $BASE64_SEC_LANDING_BUCKET"
    break
  fi
  (( retry_count+=1 ))

done


retry_count=0
while [[ "$retry_count" -lt 3 ]]; do

RESULT_PUBSUB=$(curl -s -X POST -H "Authorization: Bearer ${ACCESS}" -H "Content-Type:application/json" -d "{\"ciphertext\": \"${CIPHER_TEXT_PUBSUB}\"}" https://cloudkms.googleapis.com/v1/projects/${KMS_KEY}:decrypt)
BASE64_SEC_PUBSUB=$( echo ${RESULT_PUBSUB} | sed 's|,|\n|g' | grep -w "plaintext" | cut -d ":" -f2- | tr -d '"' )

  if [[ $BASE64_SEC_PUBSUB == '' ]]; then

    if [[ $retry_count -lt 3 ]]; then
      echo "BASE64_SEC_PUBSUB is coming as EMPTY, in attempt $retry_count!!! again try after 5 seconds"
      echo "RESULT_PUBSUB: $RESULT_PUBSUB"
      echo "BASE64_SEC_PUBSUB: $BASE64_SEC_PUBSUB"
      sleep 5
      continue
    else
      echo "BASE64_SEC_PUBSUB is EMPTY, Maximum attempt exceeded: $retry_count, exit from loop!!!"
      exit 1
    fi
  else
    echo "Successfully collected CipherText from KMS on $retry_count try, for:"
    echo "RESULT_PUBSUB: $RESULT_PUBSUB"
    echo "BASE64_SEC_PUBSUB: $BASE64_SEC_PUBSUB"
    break
  fi
  (( retry_count+=1 ))

done


retry_count=0
while [[ "$retry_count" -lt 3 ]]; do

RESULT_TRIGGER=$(curl -s -X POST -H "Authorization: Bearer ${ACCESS}" -H "Content-Type:application/json" -d "{\"ciphertext\": \"${CIPHER_TEXT_TRIGGER_BUCKET}\"}" https://cloudkms.googleapis.com/v1/projects/${KMS_KEY}:decrypt)
BASE64_SEC_TRIGGER=$( echo ${RESULT_TRIGGER} | sed 's|,|\n|g' | grep -w "plaintext" | cut -d ":" -f2- | tr -d '"' )

  if [[ $BASE64_SEC_TRIGGER == '' ]]; then

    if [[ $retry_count -lt 3 ]]; then
      echo "BASE64_SEC_TRIGGER is coming as EMPTY, in attempt $retry_count!!! again try after 5 seconds"
      echo "RESULT_TRIGGER: $RESULT_TRIGGER"
      echo "BASE64_SEC_TRIGGER: $BASE64_SEC_TRIGGER"
      sleep 5
      continue
    else
      echo "BASE64_SEC_TRIGGER is EMPTY, Maximum attempt exceeded: $retry_count, exit from loop!!!"
      exit 1
    fi
  else
    echo "Successfully collected CipherText from KMS on $retry_count try, for:"
    echo "RESULT_TRIGGER: $RESULT_TRIGGER"
    echo "BASE64_SEC_TRIGGER: $BASE64_SEC_TRIGGER"
    break
  fi
  (( retry_count+=1 ))

done



echo "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"
echo "########## Converting: BASE64 to plain text...##########"

SECRET_SOURCE=$(echo ${BASE64_SEC_SOURCE}|base64 -d)
if [[ $SECRET_SOURCE == '' ]]; then
  echo "SECRET_SOURCE coming as EMPTY"
  exit 2
else
  echo "SECRET_SOURCE: $SECRET_SOURCE"
fi

SECRET_AUDITDB=$(echo ${BASE64_SEC_AUDITDB}|base64 -d)
if [[ $SECRET_AUDITDB == '' ]]; then
  echo "SECRET_AUDITDB coming as EMPTY"
  exit 2
else
  echo "SECRET_AUDITDB: $SECRET_AUDITDB"
fi

SECRET_LANDING_BUCKET=$(echo ${BASE64_SEC_LANDING_BUCKET}|base64 -d)
if [[ $SECRET_LANDING_BUCKET == '' ]]; then
  echo "SECRET_LANDING_BUCKET coming as EMPTY"
  exit 2
else
  echo "SECRET_LANDING_BUCKET: $SECRET_LANDING_BUCKET"
fi

SECRET_PUBSUB=$(echo ${BASE64_SEC_PUBSUB}|base64 -d)
if [[ $SECRET_PUBSUB == '' ]]; then
  echo "SECRET_PUBSUB coming as EMPTY"
  exit 2
else
  echo "SECRET_PUBSUB: $SECRET_PUBSUB"
fi

SECRET_TRIGGER=$(echo ${BASE64_SEC_TRIGGER}|base64 -d)
if [[ $SECRET_TRIGGER == '' ]]; then
  echo "SECRET_TRIGGER coming as EMPTY"
  exit 2
else
  echo "SECRET_TRIGGER: $SECRET_TRIGGER"
fi



echo "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"
echo "########## Validating: Secret file exist or not...##########"

AUDITDB_SECRET_FILE=/etc/secr_dir/auditdb-secret.txt
echo $SECRET_AUDITDB > $AUDITDB_SECRET_FILE
[[ ! -f $AUDITDB_SECRET_FILE ]] && { echo "$AUDITDB_SECRET_FILE: file does not exist"; exit 3; }
[[ ! -s $AUDITDB_SECRET_FILE ]] && { echo "$AUDITDB_SECRET_FILE: file is empty"; exit 4; }

SECRET_LANDING_BUCKET_FILE=/etc/secr_dir/slb-it-hdf-landing-bucket.json
echo $SECRET_LANDING_BUCKET > $SECRET_LANDING_BUCKET_FILE 
[[ ! -f $SECRET_LANDING_BUCKET_FILE ]] && { echo "$SECRET_LANDING_BUCKET_FILE: file does not exist"; exit 3; }
[[ ! -s $SECRET_LANDING_BUCKET_FILE ]] && { echo "$SECRET_LANDING_BUCKET_FILE: file is empty"; exit 4; }

SECRET_PUBSUB_FILE=/etc/secr_dir/slb-it-hdf-pubsub.json
echo $SECRET_PUBSUB > $SECRET_PUBSUB_FILE
[[ ! -f $SECRET_PUBSUB_FILE ]] && { echo "$SECRET_PUBSUB_FILE: file does not exist"; exit 3; }
[[ ! -s $SECRET_PUBSUB_FILE ]] && { echo "$SECRET_PUBSUB_FILE: file is empty"; exit 4; }

SECRET_TRIGGER_FILE=/etc/secr_dir/slb-it-hdf-trigger.json
echo $SECRET_TRIGGER > $SECRET_TRIGGER_FILE
[[ ! -f $SECRET_TRIGGER_FILE ]] && { echo "$SECRET_TRIGGER_FILE: file does not exist"; exit 3; }
[[ ! -s $SECRET_TRIGGER_FILE ]] && { echo "$SECRET_TRIGGER_FILE: file is empty"; exit 4; }



    

SECRET_SOURCE_FILE=/etc/secr_dir/source-secret.txt
echo $SECRET_SOURCE > $SECRET_SOURCE_FILE
[[ ! -f $SECRET_SOURCE_FILE ]] && { echo "$SECRET_SOURCE_FILE: file does not exist"; exit 3; }
[[ ! -s $SECRET_SOURCE_FILE ]] && { echo "$SECRET_SOURCE_FILE: file is empty"; exit 4; }

exit 0
        
        