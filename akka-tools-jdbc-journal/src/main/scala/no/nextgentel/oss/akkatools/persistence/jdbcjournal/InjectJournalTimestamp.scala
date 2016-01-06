package no.nextgentel.oss.akkatools.persistence.jdbcjournal

import java.time.OffsetDateTime

/**
  * If your event-class extends this trait, we
  * will inject the timestamp stored in the journal when writing the event to the database
  */
trait InjectJournalTimestamp {
  // Return updated version of the payload/event with the injected timestamp included
  def withInjectedJournalTimestamp(timestamp:OffsetDateTime):Any
}
