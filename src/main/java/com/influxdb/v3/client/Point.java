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
package com.influxdb.v3.client;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.NumberFormat;
import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.influxdb.v3.client.internal.Arguments;
import com.influxdb.v3.client.write.WriteOptions;
import com.influxdb.v3.client.write.WritePrecision;


/**
 * Point defines the values that will be written to the database.
 * <a href="https://github.com/InfluxCommunity/influxdb3-java/blob/main/src/main/java/com/influxdb/v3/client/Point.java">See Go Implementation</a>.
 *
 * @author Jakub Bednar (bednar@github) (11/10/2018 11:40)
 */
@NotThreadSafe
public final class Point {

  private static final int MAX_FRACTION_DIGITS = 340;
  private static final ThreadLocal<NumberFormat> NUMBER_FORMATTER =
      ThreadLocal.withInitial(() -> {
        NumberFormat numberFormat = NumberFormat.getInstance(Locale.ENGLISH);
        numberFormat.setMaximumFractionDigits(MAX_FRACTION_DIGITS);
        numberFormat.setGroupingUsed(false);
        numberFormat.setMinimumFractionDigits(1);
        return numberFormat;
      });

  private final PointValues values;

  private Point(final PointValues values) {
    this.values = values;
  }

  /**
   * Create a new Point.
   *
   * @param measurementName the measurement name
   */
  public Point(@Nonnull final String measurementName) {
    Arguments.checkNotNull(measurementName, "measurement");

    values = new PointValues();
    values.setMeasurement(measurementName);
  }

  /**
   * Create a new Point withe specified a measurement name.
   *
   * @param measurementName the measurement name
   * @return new instance of {@link Point}
   */
  @Nonnull
  public static Point measurement(@Nonnull final String measurementName) {

    Arguments.checkNotNull(measurementName, "measurement");

    return new Point(new PointValues()).setMeasurement(measurementName);
  }

  /**
   * Create a new Point with given values.
   *
   * @param values the point values
   * @return the new Point
   * @throws Exception if measurement is missing
   */
  public static Point fromValues(final PointValues values) throws Exception {
    if (values.getMeasurement() == null) {
      throw new Exception("Missing measurement!");
    }
    return new Point(values);
  }

  /**
   *  Get measurement name.
   *
   * @return Measurement name
   */
  @Nonnull
  public String getMeasurement() {
    assert values.getMeasurement() != null;

    return values.getMeasurement();
  }

  /**
   * Updates the measurement for the point.
   *
   * @param measurement the measurement
   * @return this
   */
  @Nonnull
  public Point setMeasurement(@Nonnull final String measurement) {

    Arguments.checkNotNull(measurement, "precision");

    values.setMeasurement(measurement);

    return this;
  }

  /**
   * Get timestamp. Can be null if not set.
   *
   * @return timestamp or null
   */
  @Nullable
  public Number getTimestamp() {
    return values.getTimestamp();
  }

  /**
   * Updates the timestamp for the point.
   *
   * @param time the timestamp
   * @return this
   */
  @Nonnull
  public Point setTimestamp(@Nullable final Instant time) {
    values.setTimestamp(time);

    return this;
  }


  /**
   * Updates the timestamp for the point.
   *
   * @param time      the timestamp
   * @param precision the timestamp precision
   * @return this
   */
  @Nonnull
  public Point setTimestamp(@Nullable final Number time, @Nonnull final WritePrecision precision) {

    Arguments.checkNotNull(precision, "precision");

    values.setTimestamp(time, precision);

    return this;
  }

  /**
   * Updates the timestamp for the point.
   *
   * @param time      the timestamp
   * @param precision the timestamp precision
   * @return this
   */
  @Nonnull
  public Point setTimestamp(@Nullable final Long time, @Nonnull final WritePrecision precision) {

    return setTimestamp((Number) time, precision);
  }

  /**
   * Gets value of tag with given name. Returns null if tag not found.
   *
   * @param name   the tag name
   * @return tag value or null
   */
  @Nullable
  public String getTag(@Nonnull final String name) {
    Arguments.checkNotNull(name, "tagName");

    return values.getTag(name);
  }

  /**
   * Adds or replaces a tag value for this point.
   *
   * @param key   the tag name
   * @param value the tag value
   * @return this
   */
  @Nonnull
  public Point setTag(@Nonnull final String key, @Nullable final String value) {

    Arguments.checkNotNull(key, "tagName");

    values.setTag(key, value);

    return this;
  }

  /**
   * Adds or replaces tags for this point.
   *
   * @param tagsToAdd the Map of tags to add
   * @return this
   */
  @Nonnull
  public Point setTags(@Nonnull final Map<String, String> tagsToAdd) {

    Arguments.checkNotNull(tagsToAdd, "tagsToAdd");

    values.setTags(tagsToAdd);

    return this;
  }

  /**
   * Removes a tag with the specified name if it exists; otherwise, it does nothing.
   *
   * @param name   the tag name
   * @return this
   */
  @Nonnull
  public Point removeTag(@Nonnull final String name) {

    Arguments.checkNotNull(name, "tagName");

    values.removeTag(name);

    return this;
  }

  /**
   * Gets an array of tag names.
   *
   * @return An array of tag names
   */
  @Nonnull
  public String[] getTagNames() {
    return values.getTagNames();
  }

  /**
   * Gets the float field value associated with the specified name.
   * If the field is not present, returns null.
   *
   * @param name the field name
   * @return The float field value or null
   */
  @Nullable
  public Double getFloatField(@Nonnull final String name) throws ClassCastException {
    return getField(name, Double.class);
  }

  /**
   * Adds or replaces a float field.
   *
   * @param name  the field name
   * @param value the field value
   * @return this
   */
  @Nonnull
  public Point setFloatField(@Nonnull final String name, final double value) {
    return putField(name, value);
  }

  /**
   * Gets the integer field value associated with the specified name.
   * If the field is not present, returns null.
   *
   * @param name the field name
   * @return The integer field value or null
   */
  @Nullable
  public Long getIntegerField(@Nonnull final String name) throws ClassCastException {
    return getField(name, Long.class);
  }

  /**
   * Adds or replaces a integer field.
   *
   * @param name  the field name
   * @param value the field value
   * @return this
   */
  public Point setIntegerField(@Nonnull final String name, final long value) {
    return putField(name, value);
  }

  /**
   * Gets the string field value associated with the specified name.
   * If the field is not present, returns null.
   *
   * @param name the field name
   * @return The string field value or null
   */
  @Nullable
  public String getStringField(@Nonnull final String name) throws ClassCastException {
    return getField(name, String.class);
  }

  /**
   * Adds or replaces a string field.
   *
   * @param name  the field name
   * @param value the field value
   * @return this
   */
  public Point setStringField(@Nonnull final String name, final String value) {
    return putField(name, value);
  }

  /**
   * Gets the boolean field value associated with the specified name.
   * If the field is not present, returns null.
   *
   * @param name the field name
   * @return The boolean field value or null
   */
  @Nullable
  public Boolean getBooleanField(@Nonnull final String name) throws ClassCastException {
    return getField(name, Boolean.class);
  }

  /**
   * Adds or replaces a boolean field.
   *
   * @param name  the field name
   * @param value the field value
   * @return this
   */
  public Point setBooleanField(@Nonnull final String name, final boolean value) {
    return putField(name, value);
  }

  /**
   * Get field of given name. Can be null if field doesn't exist.
   *
   * @param name  the field name
   * @return Field as object
   */
  @Nullable
  public Object getField(@Nonnull final String name) {
    return values.getField(name);
  }

  /**
   * Get field of given name as type. Can be null if field doesn't exist.
   *
   * @param name  the field name
   * @param type  the field type Class
   * @param <T>   the field type
   * @return Field as given type
   */
  @Nullable
  public <T> T getField(final String name, final Class<T> type) throws ClassCastException {
    Object field = getField(name);
    if (field == null) {
      return null;
    }
    return type.cast(field);
  }

  /**
   * Gets the type of field with given name, if it exists.
   * If the field is not present, returns null.
   *
   * @param name  the field name
   * @return The field type or null.
   */
  @Nullable
  public Class<?> getFieldType(@Nonnull final String name) {
    Object field = getField(name);
    if (field == null) {
      return null;
    }
    return field.getClass();
  }

  /**
   * Add {@link Double} field.
   *
   * @param field the field name
   * @param value the field value
   * @return this
   */
  @Nonnull
  public Point setField(@Nonnull final String field, final double value) {
    return putField(field, value);
  }

  /**
   * Add {@link Long} field.
   *
   * @param field the field name
   * @param value the field value
   * @return this
   */
  public Point setField(@Nonnull final String field, final long value) {
    return putField(field, value);
  }

  /**
   * Add {@link Number} field.
   *
   * @param field the field name
   * @param value the field value
   * @return this
   */
  @Nonnull
  public Point setField(@Nonnull final String field, @Nullable final Number value) {
    return putField(field, value);
  }

  /**
   * Add {@link String} field.
   *
   * @param field the field name
   * @param value the field value
   * @return this
   */
  @Nonnull
  public Point setField(@Nonnull final String field, @Nullable final String value) {
    return putField(field, value);
  }

  /**
   * Add {@link Boolean} field.
   *
   * @param field the field name
   * @param value the field value
   * @return this
   */
  @Nonnull
  public Point setField(@Nonnull final String field, final boolean value) {
    return putField(field, value);
  }

  /**
   * Add {@link Object} field.
   *
   * @param field the field name
   * @param value the field value
   * @return this
   */
  @Nonnull
  public Point setField(@Nonnull final String field, @Nullable final Object value) {
    return putField(field, value);
  }

  /**
   * Adds or replaces fields for this point.
   *
   * @param fieldsToAdd the Map of fields to add
   * @return this
   */
  @Nonnull
  public Point setFields(@Nonnull final Map<String, Object> fieldsToAdd) {

    Arguments.checkNotNull(fieldsToAdd, "fieldsToAdd");

    fieldsToAdd.forEach(this::putField);

    return this;
  }

  /**
   * Removes a field with the specified name if it exists; otherwise, it does nothing.
   *
   * @param name the field name
   * @return this
   */
  @Nonnull
  public Point removeField(@NonNull final String name) {
    values.removeField(name);

    return  this;
  }

  /**
   * Gets an array of field names.
   *
   * @return An array of field names
   */
  @Nonnull
  public String[] getFieldNames() {
    return values.getFieldNames();
  }

  /**
   * Has point any fields?
   *
   * @return true, if the point contains any fields, false otherwise.
   */
  public boolean hasFields() {
    return values.hasFields();
  }

  /**
   * Creates a copy of this object.
   *
   * @return A new instance with same values.
   */
  @Nonnull
  public Point copy() {
    return new Point(values.copy());
  }

  /**
   * Transform to Line Protocol with nanosecond precision.
   *
   * @return Line Protocol
   */
  @Nonnull
  public String toLineProtocol() {
    return toLineProtocol(null);
  }

  /**
   * Transform to Line Protocol.
   *
   * @param precision required precision
   * @return Line Protocol
   */
  @Nonnull
  public String toLineProtocol(@Nullable final WritePrecision precision) {

    StringBuilder sb = new StringBuilder();

    escapeKey(sb, getMeasurement(), false);
    appendTags(sb);
    boolean appendedFields = appendFields(sb);
    if (!appendedFields) {
      return "";
    }
    appendTime(sb, precision);

    return sb.toString();
  }

  @Nonnull
  private Point putField(@Nonnull final String field, @Nullable final Object value) {

    Arguments.checkNonEmpty(field, "fieldName");

    values.setField(field, value);
    return this;
  }

  private void appendTags(@Nonnull final StringBuilder sb) {

    for (String name : values.getTagNames()) {

      String value = values.getTag(name);

      if (name.isEmpty() || value == null || value.isEmpty()) {
        continue;
      }

      sb.append(',');
      escapeKey(sb, name);
      sb.append('=');
      escapeKey(sb, value);
    }
    sb.append(' ');
  }

  private boolean appendFields(@Nonnull final StringBuilder sb) {

    boolean appended = false;

    for (String field : values.getFieldNames()) {
      Object value = values.getField(field);
      if (isNotDefined(value)) {
        continue;
      }
      escapeKey(sb, field);
      sb.append('=');
      if (value instanceof Number) {
        if (value instanceof Double || value instanceof Float || value instanceof BigDecimal) {
          sb.append(NUMBER_FORMATTER.get().format(value));
        } else {
          sb.append(value).append('i');
        }
      } else if (value instanceof String) {
        String stringValue = (String) value;
        sb.append('"');
        escapeValue(sb, stringValue);
        sb.append('"');
      } else {
        sb.append(value);
      }

      sb.append(',');

      appended = true;
    }

    // efficiently chop off the trailing comma
    int lengthMinusOne = sb.length() - 1;
    if (sb.charAt(lengthMinusOne) == ',') {
      sb.setLength(lengthMinusOne);
    }

    return appended;
  }

  private void appendTime(@Nonnull final StringBuilder sb, @Nullable final WritePrecision precision) {

    var time = getTimestamp();
    if (time == null) {
      return;
    }

    sb.append(" ");

    WritePrecision precisionNotNull = precision != null ? precision : WriteOptions.DEFAULT_WRITE_PRECISION;

    if (WritePrecision.NS.equals(precisionNotNull)) {
      if (time instanceof BigDecimal) {
        sb.append(((BigDecimal) time).toBigInteger());
      } else if (time instanceof BigInteger) {
        sb.append(time);
      } else {
        sb.append(time.longValue());
      }
    } else {
      long timeLong;
      if (time instanceof BigDecimal) {
        timeLong = ((BigDecimal) time).longValueExact();
      } else if (time instanceof BigInteger) {
        timeLong = ((BigInteger) time).longValueExact();
      } else {
        timeLong = time.longValue();
      }
      sb.append(toTimeUnit(precisionNotNull).convert(timeLong, toTimeUnit(WritePrecision.NS)));
    }
  }

  private void escapeKey(@Nonnull final StringBuilder sb, @Nonnull final String key) {
    escapeKey(sb, key, true);
  }

  private void escapeKey(@Nonnull final StringBuilder sb, @Nonnull final String key, final boolean escapeEqual) {
    for (int i = 0; i < key.length(); i++) {
      switch (key.charAt(i)) {
        case '\n':
          sb.append("\\n");
          continue;
        case '\r':
          sb.append("\\r");
          continue;
        case '\t':
          sb.append("\\t");
          continue;
        case ' ':
        case ',':
          sb.append('\\');
          break;
        case '=':
          if (escapeEqual) {
            sb.append('\\');
          }
          break;
        default:
      }

      sb.append(key.charAt(i));
    }
  }

  private void escapeValue(@Nonnull final StringBuilder sb, @Nonnull final String value) {
    for (int i = 0; i < value.length(); i++) {
      switch (value.charAt(i)) {
        case '\\':
        case '\"':
          sb.append('\\');
        default:
          sb.append(value.charAt(i));
      }
    }
  }

  private boolean isNotDefined(final Object value) {
    return value == null
        || (value instanceof Double && !Double.isFinite((Double) value))
        || (value instanceof Float && !Float.isFinite((Float) value));
  }

  @Nonnull
  private TimeUnit toTimeUnit(@Nonnull final WritePrecision precision) {
    switch (precision) {
      case MS:
        return TimeUnit.MILLISECONDS;
      case S:
        return TimeUnit.SECONDS;
      case US:
        return TimeUnit.MICROSECONDS;
      case NS:
        return TimeUnit.NANOSECONDS;
      default:
        throw new IllegalStateException("Unexpected value: " + precision);
    }
  }
}
