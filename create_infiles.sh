#!/bin/bash

ERR_USAGE=1
ERR_FILE=2
ERR_NUM=3

if [ "$#" -lt 5 ]; then
	echo "Usage: $0 diseasesFile countriesFile input_dir numFilesPerDirectory numRecordsPerFile"
	exit $ERR_USAGE
fi

DISEASES_FILE="$(readlink -f "$1")"
COUNTRIES_FILE="$(readlink -f "$2")"
DIR="$3"
FILES_PER_DIR="$4"
RECORDS_PER_FILE="$5"

# Checks

for f in "$DISEASES_FILE" "$COUNTRIES_FILE"; do
	if [ ! -f "$f" ]; then
		echo "File not found: $f"
		exit $ERR_FILE
	fi
done

INT='^[0-9]+$'
if ! [[ "$FILES_PER_DIR" =~ $INT ]]; then
	echo "numFilesPerDirectory must be >= 0!"
	exit $ERR_NUM;
elif ! [[ "$RECORDS_PER_FILE" =~ $INT ]]; then
	echo "numRecordsPerFile must be >= 0!"
	exit $ERR_NUM;
fi

# Create input_dir (top level)
mkdir -p "$DIR"
cd "$DIR"

# Create files
RECORD_ID=1
NAMES=("John" "Peter" "Eve" "Mary" "Constance" "Nick" "Sabrina" "Reginald"
       "Ronald" "Helen" "Julia" "Julianna" "Maria" "Jonas" "Ruth" "Keith" "Ian"
       "George" "Samantha" "Katerina" "Liam" "Sophie" "Irene" "Hope" "Jordan")

while read -r country; do
	mkdir -p "$country/"
	cd "$country"

	# Make required amount of date files
	for ((f=1; f<=$FILES_PER_DIR; f++)); do
		# Random date whose file doesn't already exist
		# Emulate a do..while loop
		while
			# Random date 0-9999 days ago
			file=$(date -d "$((RANDOM % 10000)) days ago" +%d-%m-%Y)

			# See README[7]
			file=${file/#31/30}

			[ -f "$file" ]
		do :; done

		# Create empty file
		> "$file"
	done

	# Indexed array of the date files for this country
	DATES=(*)

	# Array of flags to keep track of full files (reached the line limit)
	FULL=("${DATES[@]/*/0}")

	# Create pairs of ENTER, EXIT records and distribute them randomly,
	# until we run out of space.
	while [[ "${FULL[@]}" =~ 0 ]]; do
		# ENTER RECORD
		RECORD=(
			"$((RECORD_ID++))"
			"ENTER"
			"${NAMES[$RANDOM % ${#NAMES[@]}]}"
			"${NAMES[$RANDOM % ${#NAMES[@]}]}"
			"$(shuf -n 1 $DISEASES_FILE)"
			$((RANDOM % 120 + 1))
		)

		while
			f="$((RANDOM % FILES_PER_DIR))"
			[ "${FULL[$f]}" -eq 1 ]
		do :; done

		echo "${RECORD[*]}" >> "${DATES[$f]}"

		# Update flag
		if [ "$(wc -l < ${DATES[$f]})" -ge $RECORDS_PER_FILE ]; then
			FULL[$f]=1
		fi

		# Recheck for space now
		[[ "${FULL[@]}" =~ 0 ]] || break

		# EXIT RECORD
		# Note that the EXIT record can end up at an earlier date
		# than the ENTER record, which will be detected later by the
		# aggregator. In that case, only the ENTER record will be valid.
		while
			f="$((RANDOM % FILES_PER_DIR))"
			[ "${FULL[$f]}" -eq 1 ]
		do :; done

		RECORD[1]="EXIT"
		echo "${RECORD[*]}" >> "${DATES[$f]}"

		# Update flag
		if [ "$(wc -l < ${DATES[$f]})" -ge $RECORDS_PER_FILE ]; then
			FULL[$f]=1
		fi
	done

	cd ".."
done < "$COUNTRIES_FILE"
