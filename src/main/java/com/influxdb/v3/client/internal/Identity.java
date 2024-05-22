package com.influxdb.v3.client.internal;

/**
 * Functions for establishing caller identity.
 */
public final class Identity {
  private Identity() { }

  /**
   * Attempt to get the package version.
   * @return - package version or unknown.
   */
  public static String getVersion() {
    Package mainPackage = Identity.class.getPackage();
    String version = mainPackage != null ? mainPackage.getImplementationVersion() : "unknown";
    return version == null ? "unknown" : version;
  }

  /**
   * Get a standard user-agent identity to be used in all HTTP based calls.
   * @return - the standard user-agent string.
   */
  public static String getUserAgent() {
    return String.format("influxdb3-java:%s", getVersion());
  }
}
