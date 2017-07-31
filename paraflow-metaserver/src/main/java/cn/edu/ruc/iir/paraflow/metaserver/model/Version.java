package cn.edu.ruc.iir.paraflow.metaserver.model;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Table;

/**
 * ParaFlow
 *
 * @author guodong
 */
@Table("Version")
public class Version extends Model
{
    private Version()
    {
    }

    private void setVersionId(int versionId)
    {
        set("version_id", versionId);
    }

    public int getVersionId()
    {
        return getInteger("version_id");
    }

    static class Builder
    {
        private int versionId;

        public Builder setVersionId(int versionId)
        {
            this.versionId = versionId;
            return this;
        }

        public Version save()
        {
            Version v = new Version();
            v.setVersionId(this.versionId);

            return v;
        }
    }
}
