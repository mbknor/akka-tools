package no.nextgentel.oss.akkatools.logging

trait HasMdcInfo {
  def extractMdcInfo():Map[String,String]
}
