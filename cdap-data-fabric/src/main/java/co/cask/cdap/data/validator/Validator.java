/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.data.validator;

import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.format.RecordFormat;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.format.RecordFormats;
import co.cask.cdap.proto.StreamProperties;
import co.cask.cdap.proto.StreamViewProperties;

import javax.annotation.Nullable;

/**
 * Validates various objects constructed from HTTP requests.
 */
public class Validator {

  private Validator() {
  }

  public static StreamViewProperties validate(@Nullable StreamViewProperties properties) throws BadRequestException {
    if (properties == null) {
      throw new BadRequestException("Stream view properties must be specified");
    }

    return new StreamViewProperties(
      properties.getStream(), properties.getFormat() == null ? null : validateFormatSpec(properties.getFormat()));
  }

  public static StreamProperties validate(@Nullable StreamProperties properties) throws BadRequestException {
    if (properties == null) {
      throw new BadRequestException("Stream properties must be specified");
    }

    // Validate ttl
    Long ttl = properties.getTTL();
    if (ttl != null && ttl < 0) {
      throw new BadRequestException("TTL value should be positive.");
    }

    // Validate format
    FormatSpecification formatSpec = properties.getFormat() == null ?
      null : validateFormatSpec(properties.getFormat());

    // Validate notification threshold
    Integer threshold = properties.getNotificationThresholdMB();
    if (threshold != null && threshold <= 0) {
      throw new BadRequestException("Threshold value should be greater than zero.");
    }

    return new StreamProperties(ttl, formatSpec, threshold);
  }

  public static FormatSpecification validateFormatSpec(
    @Nullable FormatSpecification formatSpec) throws BadRequestException {

    if (formatSpec == null) {
      throw new BadRequestException("A format specification must be specified.");
    }

    String formatName = formatSpec.getName();
    if (formatName == null) {
      throw new BadRequestException("A format name must be specified.");
    }

    try {
      // if a format is given, make sure it is a valid format,
      // check that we can instantiate the format class
      RecordFormat<?, ?> format = RecordFormats.createInitializedFormat(formatSpec);
      // the request may contain a null schema, in which case the default schema of the format should be used.
      // create a new specification object that is guaranteed to have a non-null schema.
      return new FormatSpecification(formatSpec.getName(), format.getSchema(), formatSpec.getSettings());
    } catch (UnsupportedTypeException e) {
      throw new BadRequestException("Format " + formatName + " does not support the requested schema.");
    } catch (Exception e) {
      throw new BadRequestException("Invalid format, unable to instantiate format " + formatName);
    }
  }

}
