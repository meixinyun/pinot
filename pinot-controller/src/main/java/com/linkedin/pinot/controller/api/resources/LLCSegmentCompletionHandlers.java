/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.api.resources;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.httpclient.URI;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.common.protocols.SegmentCompletionProtocol;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.realtime.SegmentCompletionManager;
import io.swagger.annotations.Api;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;


@Api(tags = Constants.INTERNAL_TAG)
@Path("/")
public class LLCSegmentCompletionHandlers {

  private static Logger LOGGER = LoggerFactory.getLogger(LLCSegmentCompletionHandlers.class);
  private static final String SCHEME = "file://";

  @Inject
  ControllerConf _controllerConf;

  @Inject
  PinotHelixResourceManager _helixResourceManager;

  // We don't want to document these in swagger since they are internal APIs
  @GET
  @Path(SegmentCompletionProtocol.MSG_TYPE_EXTEND_BUILD_TIME)
  @Produces(MediaType.APPLICATION_JSON)
  public String extendBuildTime(
      @QueryParam(SegmentCompletionProtocol.PARAM_INSTANCE_ID) String instanceId,
      @QueryParam(SegmentCompletionProtocol.PARAM_SEGMENT_NAME) String segmentName,
      @QueryParam(SegmentCompletionProtocol.PARAM_OFFSET) long offset,
      @QueryParam(SegmentCompletionProtocol.PARAM_EXTRA_TIME_SEC) int extraTimeSec
  ) {

    if (instanceId == null || segmentName == null || offset == -1) {
      LOGGER.error("Invalid call: offset={}, segmentName={}, instanceId={}", offset, segmentName, instanceId);
      return SegmentCompletionProtocol.RESP_FAILED.toJsonString();
    }

    SegmentCompletionProtocol.Request.Params requestParams = new SegmentCompletionProtocol.Request.Params();
    requestParams.withInstanceId(instanceId).withSegmentName(segmentName).withOffset(offset).withExtraTimeSec(
        extraTimeSec);
    LOGGER.info(requestParams.toString());

    try {
      SegmentCompletionProtocol.Response response =
          SegmentCompletionManager.getInstance().extendBuildTime(requestParams);

      final String responseStr = response.toJsonString();
      LOGGER.info(responseStr);
      return responseStr;
    } catch (Throwable t) {
      LOGGER.error("Exception handling request", t);
      throw new WebApplicationException(t);
    }
  }

  @GET
  @Path(SegmentCompletionProtocol.MSG_TYPE_CONSUMED)
  @Produces(MediaType.APPLICATION_JSON)
  public String segmentConsumed(
      @QueryParam(SegmentCompletionProtocol.PARAM_INSTANCE_ID) String instanceId,
      @QueryParam(SegmentCompletionProtocol.PARAM_SEGMENT_NAME) String segmentName,
      @QueryParam(SegmentCompletionProtocol.PARAM_OFFSET) long offset,
      @QueryParam(SegmentCompletionProtocol.PARAM_REASON) String stopReason
  ) {

    if (instanceId == null || segmentName == null || offset == -1) {
      LOGGER.error("Invalid call: offset={}, segmentName={}, instanceId={}", offset, segmentName, instanceId);
      return SegmentCompletionProtocol.RESP_FAILED.toJsonString();
    }
    SegmentCompletionProtocol.Request.Params requestParams = new SegmentCompletionProtocol.Request.Params();
    requestParams.withInstanceId(instanceId).withSegmentName(segmentName).withOffset(offset).withReason(stopReason);
    LOGGER.info(requestParams.toString());

    try {
      SegmentCompletionProtocol.Response response =
          SegmentCompletionManager.getInstance().segmentConsumed(requestParams);
      final String responseStr = response.toJsonString();
      LOGGER.info(responseStr);
      return responseStr;
    } catch(Throwable t) {
      LOGGER.error("Exception handling request", t);
      throw new WebApplicationException(t);
    }
  }

  @GET
  @Path(SegmentCompletionProtocol.MSG_TYPE_STOPPED_CONSUMING)
  @Produces(MediaType.APPLICATION_JSON)
  public String segmentStoppedConsuming(
      @QueryParam(SegmentCompletionProtocol.PARAM_INSTANCE_ID) String instanceId,
      @QueryParam(SegmentCompletionProtocol.PARAM_SEGMENT_NAME) String segmentName,
      @QueryParam(SegmentCompletionProtocol.PARAM_OFFSET) long offset,
      @QueryParam(SegmentCompletionProtocol.PARAM_REASON) String stopReason
  ) {

    if (instanceId == null || segmentName == null || offset == -1) {
      LOGGER.error("Invalid call: offset={}, segmentName={}, instanceId={}", offset, segmentName, instanceId);
      return SegmentCompletionProtocol.RESP_FAILED.toJsonString();
    }
    SegmentCompletionProtocol.Request.Params requestParams = new SegmentCompletionProtocol.Request.Params();
    requestParams.withInstanceId(instanceId).withSegmentName(segmentName).withOffset(offset).withReason(stopReason);
    LOGGER.info(requestParams.toString());

    try {
      SegmentCompletionProtocol.Response response =
          SegmentCompletionManager.getInstance().segmentStoppedConsuming(requestParams);
      final String responseStr = response.toJsonString();
      LOGGER.info(responseStr);
      return responseStr;
    } catch (Throwable t) {
      LOGGER.error("Exception handling request", t);
      throw new WebApplicationException(t);
    }
  }

  @GET
  @Path(SegmentCompletionProtocol.MSG_TYPE_COMMIT_START)
  @Produces(MediaType.APPLICATION_JSON)
  public String segmentCommitStart(
      @QueryParam(SegmentCompletionProtocol.PARAM_INSTANCE_ID) String instanceId,
      @QueryParam(SegmentCompletionProtocol.PARAM_SEGMENT_NAME) String segmentName,
      @QueryParam(SegmentCompletionProtocol.PARAM_OFFSET) long offset
  ) {
    if (instanceId == null || segmentName == null || offset == -1) {
      LOGGER.error("Invalid call: offset={}, segmentName={}, instanceId={}", offset, segmentName, instanceId);
      return SegmentCompletionProtocol.RESP_FAILED.toJsonString();
    }

    SegmentCompletionProtocol.Request.Params requestParams = new SegmentCompletionProtocol.Request.Params();
    requestParams.withInstanceId(instanceId).withSegmentName(segmentName).withOffset(offset);
    LOGGER.info(requestParams.toString());

    try {
      SegmentCompletionProtocol.Response response =
          SegmentCompletionManager.getInstance().segmentCommitStart(requestParams);
      final String responseStr = response.toJsonString();
      LOGGER.info(responseStr);
      return responseStr;
    } catch (Throwable t) {
      LOGGER.error("Exception handling request", t);
      throw new WebApplicationException(t);
    }
  }

  @GET
  @Path(SegmentCompletionProtocol.MSG_TYPE_COMMIT_START)
  @Produces(MediaType.APPLICATION_JSON)
  public String segmentCommitEnd(
      @QueryParam(SegmentCompletionProtocol.PARAM_INSTANCE_ID) String instanceId,
      @QueryParam(SegmentCompletionProtocol.PARAM_SEGMENT_NAME) String segmentName,
      @QueryParam(SegmentCompletionProtocol.PARAM_OFFSET) long offset
  ) {
    if (instanceId == null || segmentName == null || offset == -1) {
      LOGGER.error("Invalid call: offset={}, segmentName={}, instanceId={}", offset, segmentName, instanceId);
      return SegmentCompletionProtocol.RESP_FAILED.toJsonString();
    }

    SegmentCompletionProtocol.Request.Params requestParams = new SegmentCompletionProtocol.Request.Params();
    requestParams.withInstanceId(instanceId).withSegmentName(segmentName).withOffset(offset);
    LOGGER.info(requestParams.toString());

    final boolean isSuccess = true;
    final boolean isSplitCommit = true;

    try {
      SegmentCompletionProtocol.Response response =
          SegmentCompletionManager.getInstance().segmentCommitEnd(requestParams, isSuccess, isSplitCommit);
      final String responseStr = response.toJsonString();
      LOGGER.info(responseStr);
      return responseStr;
    } catch (Throwable t) {
      LOGGER.error("Exception handling request", t);
      throw new WebApplicationException(t);
    }
  }

  @POST
  @Path(SegmentCompletionProtocol.MSG_TYPE_COMMIT)
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Produces(MediaType.APPLICATION_JSON)
  public String segmentCommit(
      @QueryParam(SegmentCompletionProtocol.PARAM_INSTANCE_ID) String instanceId,
      @QueryParam(SegmentCompletionProtocol.PARAM_SEGMENT_NAME) String segmentName,
      @QueryParam(SegmentCompletionProtocol.PARAM_OFFSET) long offset,
      FormDataMultiPart multiPart
  )
  {
    SegmentCompletionProtocol.Request.Params requestParams = new SegmentCompletionProtocol.Request.Params();
    requestParams.withInstanceId(instanceId).withSegmentName(segmentName).withOffset(offset);
    LOGGER.info(requestParams.toString());

    final SegmentCompletionManager segmentCompletionManager = SegmentCompletionManager.getInstance();
    SegmentCompletionProtocol.Response response = segmentCompletionManager.segmentCommitStart(requestParams);
    if (response.equals(SegmentCompletionProtocol.RESP_COMMIT_CONTINUE)) {
      // Get the segment and put it in the right place.
      boolean success = uploadSegment(multiPart, instanceId, segmentName, false) != null;

      response = segmentCompletionManager.segmentCommitEnd(requestParams, success, false);
    }

    LOGGER.info("Response: instance={}  segment={} status={} offset={}", requestParams.getInstanceId(), requestParams.getSegmentName(),
        response.getStatus(), response.getOffset());

    return response.toJsonString();
  }

  // This method may be called in any controller, leader or non-leader.
  @POST
  @Path(SegmentCompletionProtocol.MSG_TYPE_SEGMENT_UPLOAD)
  @Produces(MediaType.APPLICATION_JSON)
  public String segmentUpload(
      @QueryParam(SegmentCompletionProtocol.PARAM_INSTANCE_ID) String instanceId,
      @QueryParam(SegmentCompletionProtocol.PARAM_SEGMENT_NAME) String segmentName,
      @QueryParam(SegmentCompletionProtocol.PARAM_OFFSET) long offset,
      FormDataMultiPart multiPart
  )
  {
    SegmentCompletionProtocol.Request.Params requestParams = new SegmentCompletionProtocol.Request.Params();
    requestParams.withInstanceId(instanceId).withSegmentName(segmentName).withOffset(offset);
    LOGGER.info(requestParams.toString());

    final String segmentLocation = uploadSegment(multiPart, instanceId, segmentName, true);
    if (segmentLocation == null) {
      return SegmentCompletionProtocol.RESP_FAILED.toJsonString();
    }
    SegmentCompletionProtocol.Response.Params responseParams = new SegmentCompletionProtocol.Response.Params()
        .withOffset(requestParams.getOffset())
        .withSegmentLocation(segmentLocation)
        .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.UPLOAD_SUCCESS);

    return new SegmentCompletionProtocol.Response(responseParams).toJsonString();
  }

  private String uploadSegment(FormDataMultiPart multiPart, final String instanceId, final String segmentName,
      boolean isSplitCommit) {
    Map<String, List<FormDataBodyPart>> map = multiPart.getFields();
    if (!validateMultiPart(map, segmentName)) {
      return null;
    }
    final String name = map.keySet().iterator().next();
    final FormDataBodyPart bodyPart = map.get(name).get(0);
    InputStream is = bodyPart.getValueAs(InputStream.class);
    String segmentLocation;

    FileOutputStream os = null;
    try {
      FileUploadPathProvider provider = new FileUploadPathProvider(_controllerConf);
      File tmpFile = new File(provider.getTmpDir(), name + "." + UUID.randomUUID().toString());
      tmpFile.deleteOnExit();
      os = new FileOutputStream(tmpFile);
      IOUtils.copyLarge(is, os);
      os.flush();
      LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
      final String rawTableName = llcSegmentName.getTableName();
      final File tableDir = new File(provider.getBaseDataDir(), rawTableName);
      File segmentFile;
      if (isSplitCommit) {
        String uniqueSegmentFileName = SegmentCompletionUtils.generateSegmentFileName(segmentName);
        segmentFile = new File(tableDir, uniqueSegmentFileName);
      } else {
        segmentFile = new File(tableDir, segmentName);
      }

      if (isSplitCommit) {
        FileUtils.moveFile(tmpFile, segmentFile);
      } else {
        // Multiple threads can reach this point at the same time, if the following scenario happens
        // The server that was asked to commit did so very slowly (due to network speeds). Meanwhile the FSM in
        // SegmentCompletionManager timed out, and allowed another server to commit, which did so very quickly (somehow
        // the network speeds changed). The second server made it through the FSM and reached this point.
        // The synchronization below takes care that exactly one file gets moved in place.
        // There are still corner conditions that are not handled correctly. For example,
        // 1. What if the offset of the faster server was different?
        // 2. We know that only the faster server will get to complete the COMMIT call successfully. But it is possible
        //    that the race to this statement is won by the slower server, and so the real segment that is in there is that
        //    of the slower server.
        // In order to overcome controller restarts after the segment is renamed, but before it is committed, we DO need to
        // check for existing segment file and remove it. So, the block cannot be removed altogether.
        // For now, we live with these corner cases. Once we have split-commit enabled and working, this code will no longer
        // be used.
        synchronized (SegmentCompletionManager.getInstance()) {
          if (segmentFile.exists()) {
            LOGGER.warn("Segment file {} exists. Replacing with upload from {}", segmentFile.getAbsolutePath(), instanceId);
            FileUtils.deleteQuietly(segmentFile);
          }
          FileUtils.moveFile(tmpFile, segmentFile);
        }
      }
      LOGGER.info("Moved file {} to {}", tmpFile.getAbsolutePath(), segmentFile.getAbsolutePath());
      segmentLocation = new URI(SCHEME + segmentFile.getAbsolutePath(), /* boolean escaped */ false).toString();
    } catch (InvalidControllerConfigException e) {
      LOGGER.error("Invalid controller config exception from instance {} for segment {}", instanceId, segmentName, e);
      return null;
    } catch (IOException e) {
      LOGGER.error("File upload exception from instance {} for segment {}", instanceId, segmentName, e);
      return null;
    }
    finally {
      try {
        is.close();
        if (os != null) {
          os.close();
        }
      } catch (IOException e) {
        LOGGER.error("Could not close input or output streams: instance {}, segment {}", instanceId, segmentName);
      }
    }
    return segmentLocation;
  }

  // Validate that there is one file that is in the input.
  private boolean validateMultiPart(Map<String, List<FormDataBodyPart>> map, String segmentName) {
    if (map.size() != 1) {
      LOGGER.error("Incorrect number of multi-part elements: {} for segment {}", map.size(), segmentName);
      return false;
    }
    List<FormDataBodyPart> bodyParts = map.get(map.keySet().iterator().next());
    if (bodyParts.size() != 1) {
      LOGGER.error("Incorrect number of elements in list in first part: {} for segment {}", bodyParts.size(), segmentName);
      return false;
    }
    return true;
  }
}
