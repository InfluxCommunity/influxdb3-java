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

import java.math.BigInteger;
import java.time.Instant;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.influxdb.v3.client.internal.Arguments;
import com.influxdb.v3.client.internal.NanosecondConverter;
import com.influxdb.v3.client.write.WritePrecision;


/**
 * PointValues defines the values that will be written to the database.
 * <a href="http://bit.ly/influxdata-point">See Go Implementation</a>.
 *
 * @author Jakub Bednar (bednar@github) (11/10/2018 11:40)
 */
@NotThreadSafe
public final class PointValues {

    private String name;
    private final Map<String, String> tags = new TreeMap<>();
    private final Map<String, Object> fields = new TreeMap<>();
    private Number time;

    /**
     * Create a new PointValues.
     */
    public PointValues() { }

    /**
     * Create a new PointValues withe specified a measurement name.
     *
     * @param measurementName the measurement name
     * @return new instance of {@link PointValues}
     */
    @Nonnull
    public static PointValues measurement(@Nonnull final String measurementName) {

        Arguments.checkNotNull(measurementName, "measurement");

        return new PointValues().setMeasurement(measurementName);
    }

  /**
   *  Get measurement name.
   *
   * @return Measurement name
   */
  @Nullable
  public String getMeasurement() {
    return name;
  }

  /**
   * Updates the measurement for the point.
   *
   * @param measurement the measurement
   * @return this
   */
  @Nonnull
  public PointValues setMeasurement(@Nonnull final String measurement) {

    Arguments.checkNotNull(measurement, "precision");

    this.name = measurement;

    return this;
  }

  /**
   * Get timestamp. Can be null if not set.
   *
   * @return timestamp or null
   */
  @Nullable
  public Number getTimestamp() {
    return time;
  }

  /**
   * Updates the timestamp for the point.
   *
   * @param time the timestamp
   * @return this
   */
  @Nonnull
  public PointValues setTimestamp(@Nullable final Instant time) {

    if (time == null) {
      return setTimestamp(null, WritePrecision.NS);
    }

    BigInteger convertedTime = NanosecondConverter.convert(time, WritePrecision.NS);

    return setTimestamp(convertedTime, WritePrecision.NS);
  }


  /**
   * Updates the timestamp for the point.
   *
   * @param time      the timestamp
   * @param precision the timestamp precision
   * @return this
   */
  @Nonnull
  public PointValues setTimestamp(@Nullable final Number time, @Nonnull final WritePrecision precision) {

    Arguments.checkNotNull(precision, "precision");

    this.time = NanosecondConverter.convertToNanos(time, precision);

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
  public PointValues setTimestamp(@Nullable final Long time, @Nonnull final WritePrecision precision) {

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

    return tags.get(name);
  }

  /**
   * Adds or replaces a tag value for this point.
   *
   * @param key   the tag name
   * @param value the tag value
   * @return this
   */
  @Nonnull
  public PointValues setTag(@Nonnull final String key, @Nullable final String value) {

    Arguments.checkNotNull(key, "tagName");

    tags.put(key, value);

    return this;
  }

  /**
   * Adds or replaces tags for this point.
   *
   * @param tagsToAdd the Map of tags to add
   * @return this
   */
  @Nonnull
  public PointValues setTags(@Nonnull final Map<String, String> tagsToAdd) {

    Arguments.checkNotNull(tagsToAdd, "tagsToAdd");

    tagsToAdd.forEach(this::setTag);

    return this;
  }

  /**
   * Removes a tag with the specified name if it exists; otherwise, it does nothing.
   *
   * @param name   the tag name
   * @return this
   */
  @Nonnull
  public PointValues removeTag(@Nonnull final String name) {

    Arguments.checkNotNull(name, "tagName");

    tags.remove(name);

    return this;
  }

  /**
   * Gets an array of tag names.
   *
   * @return An array of tag names
   */
  @Nonnull
    public String[] getTagNames() {
    return tags.keySet().toArray(new String[0]);
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
  public PointValues setFloatField(@Nonnull final String name, final double value) {
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
  public PointValues setIntegerField(@Nonnull final String name, final long value) {
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
  public PointValues setStringField(@Nonnull final String name, final String value) {
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
  public PointValues setBooleanField(@Nonnull final String name, final boolean value) {
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
    return  fields.get(name);
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
  public PointValues setField(@Nonnull final String field, final double value) {
    return putField(field, value);
  }

  /**
   * Add {@link Long} field.
   *
   * @param field the field name
   * @param value the field value
   * @return this
   */
  public PointValues setField(@Nonnull final String field, final long value) {
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
  public PointValues setField(@Nonnull final String field, @Nullable final Number value) {
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
  public PointValues setField(@Nonnull final String field, @Nullable final String value) {
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
  public PointValues setField(@Nonnull final String field, final boolean value) {
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
  public PointValues setField(@Nonnull final String field, @Nullable final Object value) {
    return putField(field, value);
  }

  /**
   * Adds or replaces fields for this point.
   *
   * @param fieldsToAdd the Map of fields to add
   * @return this
   */
  @Nonnull
  public PointValues setFields(@Nonnull final Map<String, Object> fieldsToAdd) {

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
  public PointValues removeField(@NonNull final String name) {
    fields.remove(name);

    return  this;
  }

  /**
   * Gets an array of field names.
   *
   * @return An array of field names
   */
  @Nonnull
  public String[] getFieldNames() {
    return fields.keySet().toArray(new String[0]);
  }

  /**
   * Has point any fields?
   *
   * @return true, if the point contains any fields, false otherwise.
   */
  public boolean hasFields() {
    return !fields.isEmpty();
  }

  /**
   * Creates a copy of this object.
   *
   * @return A new instance with same values.
   */
  @Nonnull
  public PointValues copy() {
    PointValues copy = new PointValues();

    copy.name = this.name;
    copy.tags.putAll(this.tags);
    copy.fields.putAll(this.fields);
    copy.time = this.time;

    return copy;
  }

  /**
   * Creates new Point with this as values with given measurement.
   *
   * @param measurement the point measurement
   * @return Point from this values with given measurement.
   */
  @Nonnull
  public Point asPoint(@Nonnull final String measurement) {
    setMeasurement(measurement);
    try {
      return asPoint();
    } catch (Exception e) {
      // never
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates new Point with this as values.
   *
   * @return Point from this values with given measurement.
   * @throws Exception if measurement is missing
   */
  @Nonnull
  public Point asPoint() throws Exception {
    return Point.fromValues(this);
  }

    @Nonnull
    private PointValues putField(@Nonnull final String field, @Nullable final Object value) {

        Arguments.checkNonEmpty(field, "fieldName");

        fields.put(field, value);
        return this;
    }

}
