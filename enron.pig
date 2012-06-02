register /me/pig/contrib/piggybank/java/piggybank.jar

register /me/pig/build/ivy/lib/Pig/avro-1.5.3.jar
register /me/pig/build/ivy/lib/Pig/json-simple-1.1.jar
register /me/pig/build/ivy/lib/Pig/joda-time-1.6.jar

define AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();

set default_parallel 10
set aggregate.warning true

rmf /enron/from_to_date

emails = load '/enron/emails.avro' using AvroStorage();

/* We can view our schema with describe. */
describe emails

/* Get samples of the data instantly with illustrate! */
illustrate emails

froms = foreach emails generate flatten(from.address) as from_address, flatten(tos.address) as to_address, date;
froms = filter froms by to_address is not null and to_address != '';

illustrate froms

store froms into '/enron/from_to_date';
