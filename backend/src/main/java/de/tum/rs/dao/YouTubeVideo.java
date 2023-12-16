package de.tum.rs.dao;


import com.google.api.services.youtube.model.ContentRating;
import com.google.api.services.youtube.model.VideoTopicDetails;
import java.util.List;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;

@Data
@Document(indexName = "videos")
public class YouTubeVideo{

	@Id
	private String id;

	private String etag;
	private String kind;
	private Snippet snippet;
	private Statistics statistics;
	private Status status;
	private ContentDetails contentDetails;
	private VideoTopicDetails topicDetails;

	@Data
	static class Snippet {
		private String categoryId;
		private String channelId;
		private String channelTitle;
		private String defaultAudioLanguage;
		private String defaultLanguage;
		private String description;
		private String liveBroadcastContent;
		private Localized localized;
		private PublishedAt publishedAt;
		private List<String> tags;
		private Thumbnails thumbnails;
		private String title;
	}

	@Data
	static class Localized {
		private String description;
		private String title;
	}

	@Data
	static class PublishedAt {
		private Long value;
		private Boolean dateOnly;
		private Integer timeZoneShift;
	}

	@Data
	static class Thumbnails {
		private Thumbnail defaultThumbnail;
		private Thumbnail high;
		private Thumbnail maxres;
		private Thumbnail medium;
		private Thumbnail standard;
	}

	@Data
	static class Thumbnail {
		private Integer height;
		private String url;
		private Integer width;
	}

	@Data
	static class Statistics {
		private Long commentCount;
		private Long favoriteCount;
		private Long likeCount;
		private Long viewCount;
	}

	@Data
	static class Status {
		private Boolean embeddable;
		private String license;
		private Boolean madeForKids;
		private String privacyStatus;
		private Boolean publicStatsViewable;
		private String uploadStatus;
	}

	@Data
	static class ContentDetails {
		private String caption;
		private ContentRating contentRating;
		private String definition;
		private String dimension;
		private String duration;
		private Boolean licensedContent;
		private String projection;
	}
}

