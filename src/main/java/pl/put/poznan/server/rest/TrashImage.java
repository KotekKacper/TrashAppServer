package pl.put.poznan.server.rest;

import java.io.Serializable;

public class TrashImage implements Serializable {

    private Long trashId;

    private String contentType;

    private byte[] content;

    public TrashImage(Long trashId, String contentType, byte[] content) {
        this.trashId = trashId;
        this.contentType = contentType;
        this.content = content;
    }


    public Long getTrashId() {
        return trashId;
    }

    public void setTrashId(Long trashId) {
        this.trashId = trashId;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.content = content;
    }
}

