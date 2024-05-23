/*
 * The MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.influxdb.v3.client.internal;

/**
 * Functions for establishing caller identity.
 */
final class Identity {
  private Identity() { }

  /**
   * Attempt to get the package version.
   * @return - package version or unknown.
   */
  static String getVersion() {
    Package mainPackage = Identity.class.getPackage();
    String version = mainPackage != null ? mainPackage.getImplementationVersion() : "unknown";
    return version == null ? "unknown" : version;
  }

  /**
   * Get a standard user-agent identity to be used in all HTTP based calls.
   * @return - the standard user-agent string.
   */
  static String getUserAgent() {
    return String.format("influxdb3-java/%s", getVersion());
  }
}